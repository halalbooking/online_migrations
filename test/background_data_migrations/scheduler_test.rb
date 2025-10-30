# frozen_string_literal: true

require "test_helper"

module BackgroundDataMigrations
  class SchedulerTest < Minitest::Test
    def setup
      @connection = ActiveRecord::Base.connection
      @connection.create_table(:users, force: :cascade) do |t|
        t.boolean :admin
      end

      User.reset_column_information
    end

    def teardown
      @connection.drop_table(:users, if_exists: true)
      OnlineMigrations::BackgroundDataMigrations::Migration.delete_all
    end

    def test_run
      m = create_migration(migration_name: "MakeAllNonAdmins")

      run_scheduler

      assert_enqueued_with(job: OnlineMigrations::BackgroundDataMigrations::MigrationJob, args: [m.id])

      m.reload
      assert m.running?
      assert_not_nil m.jid
    end

    def test_run_specific_shard
      m1 = create_migration(migration_name: "MakeAllDogsNice", shard: :shard_one)
      m2 = create_migration(migration_name: "MakeAllDogsNice", shard: :shard_two)

      run_scheduler(shard: :shard_two)

      assert_enqueued_with(job: OnlineMigrations::BackgroundDataMigrations::MigrationJob, args: [m2.id])

      m2.reload
      assert m2.running?
      assert_not_nil m2.jid

      m1.reload
      assert m1.enqueued?
      assert_nil m1.jid
    end

    def test_run_no_more_than_concurrency
      m1 = create_migration(migration_name: "MakeAllNonAdmins")
      m2 = create_migration(migration_name: "MigrationWithCount")

      scheduler = OnlineMigrations::BackgroundDataMigrations::Scheduler.new
      scheduler.run(concurrency: 1)

      assert_enqueued_jobs 1, only: OnlineMigrations::BackgroundDataMigrations::MigrationJob
      assert m1.reload.running?
      assert m2.reload.enqueued?

      run_scheduler(concurrency: 2)

      assert_enqueued_jobs 2, only: OnlineMigrations::BackgroundDataMigrations::MigrationJob
      assert m1.reload.running?
      assert m2.reload.running?
    end

    class CustomJob < OnlineMigrations::BackgroundDataMigrations::MigrationJob
    end

    def test_custom_migration_job
      OnlineMigrations.config.background_data_migrations.stub(:job, CustomJob.name) do
        m = create_migration(migration_name: "MakeAllNonAdmins")
        run_scheduler

        assert_enqueued_jobs 1, only: CustomJob
        assert m.reload.running?
      end
    end

    private
      def create_migration(migration_name:, **attributes)
        OnlineMigrations::BackgroundDataMigrations::Migration.create!(migration_name: migration_name, **attributes)
      end

      def run_scheduler(**options)
        scheduler = OnlineMigrations::BackgroundDataMigrations::Scheduler.new
        scheduler.run(**options)
      end
  end
end
