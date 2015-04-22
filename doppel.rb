class Doppel
  def initialize(job)
    @doppel_schema = DoppelSchema.new(job)
    @doppel_writer = DoppelWriter.new(job)
    @doppel_builder = DoppelBuilder.new(job)
    @doppel_restore = DoppelRestore.new(job)
  end

  #Preferred means of running Doppel. The individual steps are exposed as public methods to allow intervention.
  def run(instance = nil)
    report_start
    build_data_structure instance
    create_dump_file
    create_schema
    restore
    report_completion
  end

  def build_data_structure(instance = nil)
    @doppel_builder.run instance
  end

  def config
    @doppel_builder.config
  end

  def create_schema
    @doppel_schema.create
  end

  def create_dump_file
    @doppel_writer.run records
  end

  def records
    @doppel_builder.records
  end

  def restore
    @doppel_restore.run
  end

  private

  def identify_myself
    @my_name ||= "Doppel(:#{config.job_name})"
  end

  def report_completion
    @finished ||= Time.now
    report_start
    logit "#{identify_myself} completed at #{@finished}", level: :info
    logit "#{identify_myself} total time (seconds): #{@finished - @started}", level: :info
  end

  def report_start
    @started ||= Time.now
    logit "#{identify_myself} started at #{@started}", level: :info
  end
end

