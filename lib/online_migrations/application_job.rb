# frozen_string_literal: true

require "active_job"

module OnlineMigrations
  # Base class for background jobs in the OnlineMigrations gem.
  class ApplicationJob < ActiveJob::Base
  end
end
