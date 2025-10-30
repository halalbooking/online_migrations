# frozen_string_literal: true

require "job-iteration"

module OnlineMigrations
  module BackgroundDataMigrations
    # ActiveJob job responsible for running background data migrations.
    class MigrationJob < OnlineMigrations::ApplicationJob
      include JobIteration::Iteration

      queue_as do
        OnlineMigrations.config.background_data_migrations.queue_name || :default
      end

      # Don't retry validation errors - let them bubble up
      discard_on ArgumentError

      # For runtime errors, use a high retry count and handle custom logic in rescue_from
      # We'll check max_attempts manually in the rescue block
      retry_on StandardError, wait: :exponentially_longer, attempts: 25

      rescue_from StandardError do |exception|
        migration_id = arguments.first
        migration = Migration.find(migration_id)

        # Check if we've exceeded the migration's max_attempts
        if executions >= migration.max_attempts
          # Retries exhausted - persist error and call error handler
          migration.persist_error(exception)
          OnlineMigrations.config.background_data_migrations.error_handler.call(exception, migration)
          # Don't re-raise - job is done
        else
          # Still have retries left - re-raise to trigger retry
          raise
        end
      end

      TICKER_INTERVAL = 5 # seconds

      # Register callbacks using job-iteration's callback system
      on_start do
        @migration.start if @migration
      end

      on_shutdown do
        @ticker.persist if @ticker
        @migration.stop if @migration
      end

      on_complete do
        # Job was manually cancelled.
        @migration.cancel if @migration && cancelled?
        @migration.complete if @migration
      end

      def initialize(*args)
        super

        @migration = nil
        @data_migration = nil

        @ticker = Ticker.new(TICKER_INTERVAL) do |ticks, duration|
          @migration.persist_progress(cursor_position, ticks, duration)
          @migration.reload
        end

        @throttle_checked_at = current_time
      end

      def build_enumerator(migration_id, cursor:)
        @migration = BackgroundDataMigrations::Migration.find(migration_id)
        cursor ||= @migration.cursor

        @migration.on_shard_if_present do
          @data_migration = @migration.data_migration

          # Call after_resume if we're resuming (cursor is not nil/empty)
          @data_migration.after_resume if cursor.present?

          collection_enum = @data_migration.build_enumerator(cursor: cursor)

          if collection_enum
            if !collection_enum.is_a?(Enumerator)
              raise ArgumentError, <<~MSG.squish
                #{@data_migration.class.name}#build_enumerator must return an Enumerator,
                got #{collection_enum.class.name}.
              MSG
            end

            # Return the enumerator directly - job-iteration will wrap it
            collection_enum
          else
            collection = @data_migration.collection

            case collection
            when ActiveRecord::Relation
              enumerator_builder.build_active_record_enumerator_on_records(
                collection,
                cursor: cursor,
                batch_size: @data_migration.class.active_record_enumerator_batch_size || 100
              )
            when ActiveRecord::Batches::BatchEnumerator
              if collection.start || collection.finish
                raise ArgumentError, <<~MSG.squish
                  #{@data_migration.class.name}#collection does not support
                  a batch enumerator with the "start" or "finish" options.
                MSG
              end

              # NOTE: job-iteration doesn't support use_ranges parameter
              # It always uses ranges-based iteration (equivalent to use_ranges: true)
              enumerator_builder.build_active_record_enumerator_on_batch_relations(
                collection.relation,
                cursor: cursor,
                batch_size: collection.batch_size
              )
            when Array
              enumerator_builder.build_array_enumerator(collection, cursor: cursor)
            else
              raise ArgumentError, <<~MSG.squish
                #{@data_migration.class.name}#collection must be either an ActiveRecord::Relation,
                ActiveRecord::Batches::BatchEnumerator, or Array.
              MSG
            end
          end
        end
      end

      def each_iteration(item, _migration_id)
        if @migration.cancelling? || @migration.pausing? || @migration.paused?
          # Finish this exact job. When the migration is paused
          # and will be resumed, a new job will be enqueued.
          # Manually persist progress before aborting
          current_cursor = (cursor_position || 0).is_a?(Integer) ? ((cursor_position || -1) + 1) : cursor_position
          @migration.persist_progress(current_cursor, @ticker.instance_variable_get(:@ticks_recorded), 0)
          # Reset ticker so on_shutdown doesn't overwrite the cursor we just saved
          @ticker.instance_variable_set(:@ticks_recorded, 0)
          finished = true
          throw :abort, finished
        elsif should_throttle?
          ActiveSupport::Notifications.instrument("throttled.background_data_migrations", migration: @migration)
          # Manually persist progress before aborting
          # For array/relation enumerators, cursor_position is the index we resumed from,
          # so the current item's index is cursor_position + 1 (or 1 if cursor_position is nil)
          current_cursor = (cursor_position || 0).is_a?(Integer) ? ((cursor_position || -1) + 1) : cursor_position
          @migration.persist_progress(current_cursor, @ticker.instance_variable_get(:@ticks_recorded), 0)
          # Reset ticker so on_shutdown doesn't overwrite the cursor we just saved
          @ticker.instance_variable_set(:@ticks_recorded, 0)
          finished = false
          throw :abort, finished
        else
          @data_migration.around_process do
            @migration.data_migration.process(item)

            # Migration is refreshed regularly by ticker.
            pause = @migration.iteration_pause
            sleep(pause) if pause > 0
          end
          @ticker.tick
        end
      end

      private
        # It would be better to have a callback like `around_perform`,
        # but currently this is the way to make job iteration shard aware.
        def iterate_with_enumerator(enumerator, arguments)
          @migration.on_shard_if_present { super }
        end

        THROTTLE_CHECK_INTERVAL = 5 # seconds
        private_constant :THROTTLE_CHECK_INTERVAL

        def should_throttle?
          if current_time - @throttle_checked_at >= THROTTLE_CHECK_INTERVAL
            @throttle_checked_at = current_time
            OnlineMigrations.config.throttler.call
          else
            false
          end
        end

        def current_time
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end

        # Check if job was manually cancelled
        def cancelled?
          # In job-iteration, we track this through migration state
          @migration.cancelling? || @migration.cancelled?
        end
    end
  end
end
