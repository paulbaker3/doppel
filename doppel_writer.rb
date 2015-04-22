class DoppelWriter
  require 'set'

  def initialize(job)
    @doppel_config = DoppelConfig.new(job)
    @user = @doppel_config.authorizing_user
    @requester = create_requester
  end

  def run(records)
    clear_existing_file doppel_file_name
    process records
    report_sql_file_creation
  end

  protected

  def authorized?(object)
    return true if @doppel_config.pre_authorized.include?(class_of(object))
    @requester.can?("view", object)
  end

  private

  def doppel_file_name
    @doppel_config.doppel_file_name
  end

  def report_sql_file_creation
    logit "Dump file successfully created at #{doppel_file_name}", level: :info
  end

  def blacklisted?(klass)
    @doppel_config.blacklisted.include?(klass)
  end

  def clear_existing_file(file)
    FileUtils.rm_f file
  end
  #
  def process(records)
    records.each do |class_name, ids|
      next if blacklisted?(class_name)
      klass = retrieve_model(class_name)
      objects = retrieve_objects(klass, ids)
      write objects
    end
  end

  def retrieve_model(class_name)
    Kernel.const_get(class_name)
    rescue
      raise "'#{class_name}' is not a valid model name. Ensure #{@doppel_config.config_file_name} specifies classes in CamelCase."
  end

  def retrieve_objects(klass, set)
    objects = []
    # this needs to be broken into chunks of arrays for performance reasons i think
    # find_all_by_id([array_of_ids])
    # some more notes: s
      # Firm.reflect_on_all_associations.each{|x| puts "#{x.klass.name} #{x.association_foreign_key}"}
    set.each { |id| add_to objects, klass.find(id) }
    objects
  end

  def add_to(array, object)
    if authorized?(object)
      array << object
    else
      logit "Access to #{object.class.name} #{object.id} is not authorized. Skipping.", level: :warn, color: :red
    end
    array
  end

  def write(enum, opts = {})
    enum = [enum] unless iterable?(enum)
    return if empty? enum
    table_definition_hash = define_table_structure enum.first, opts[:ignored_columns]
    insert_lines = create_insert_lines enum, table_definition_hash
    create_copy_statements insert_lines, table_definition_hash
  end

  def create_requester
    fail "Supply a :authorizing_user_id in #{@doppel_config.config_file_name}" unless @user.is_a?(User)
    Permission.new(user: @user)
  end

  def empty?(enum)
    if enum.nil? || enum.first.nil?
      Rails.logger.info "SqlDump aborted. Nothing to insert."
      return true
    else
      return false
    end
  end

  def define_table_structure(model, ignored_columns=[])
    table_structure = {}
    klass = model.class
    table_structure[:table_name] = klass.table_name
    table_structure[:columns] = column_definitions klass, ignored_columns
    table_structure
  end

  def column_definitions(klass, ignored_columns)
    ignored_columns ||= []
    fail ArgumentError.new(':ignored_columns must be an array') unless ignored_columns.is_a? Array
    cols_to_remove = ignored_columns.map(&:to_s)
    klass.column_names - cols_to_remove
  end

  def create_insert_lines(enum, table_structure)
    keys = table_structure[:columns]
    new_lines = []

    str = ''
    enum.each do |ra|
      keys.each_with_index do |k, i|
        str += "#{clean_values_for_psql ra[k]}"
        str += "\t" unless i == (keys.length - 1)
      end
      str += "\n"
      new_lines << str
      str = ''
    end
    new_lines
  end

  def clean_values_for_psql(input)
    return input unless input.is_a?(String)
    input.gsub!(/\r/, '\\r')
    input.gsub!(/\n/, '\\n')
    input
  end

  def create_copy_statements(insert_lines, table_structure)
    escaped_cols = table_structure[:columns].map { |c| "\"#{c}\"" }
    sql_statement = "COPY #{table_structure[:table_name]} (#{escaped_cols.join(', ')}) FROM STDIN WITH NULL AS '';\n"
    insert_lines.each do |line|
      sql_statement += line
    end
    sql_statement += "\\.\n"
    File.open(@doppel_config.doppel_file_name, 'a') { |file| file.write(sql_statement) }
  end

  def iterable?(object)
    object.respond_to? :each
  end

  def class_of(object)
    object.class.name
  end
end
