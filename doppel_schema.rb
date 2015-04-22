class DoppelSchema
  def initialize(job)
    @doppel_config = DoppelConfig.new(job)
  end

  def create
    FileUtils.cp original_structure_sql_file, clean_structure_sql_file
    clean_structure_sql
  end

  private

  def original_structure_sql_file
    @doppel_config.structure_sql_path
  end

  def clean_structure_sql_file
    @doppel_config.doppel_schema_name
  end

  def clean_structure_sql
    schema_file = clean_structure_sql_file
    fail "#{schema_file} does not exist" unless File.exist?(schema_file)
    if input = File.open(schema_file)
      # Create a temp file, read through the original, commenting out the target lines.
      iterate_through input
      update_file schema_file
    end
  end

  def iterate_through(input)
    @lines = Array.new
    @exclusions = lines_to_strike
    @line_count = 0
    input.each_line do |line|
      next unless line
      @line_count += 1
      process line
    end
    input.close
  end

  def update_file(schema_file)
    if @lines.count > @line_count
      # Lines were commented out, so write the new content to the file
      File.write(schema_file, @lines.join)
      return true
    else
      # No lines were commented out, so there is no need to rewrite the file
      logit "No changes are needed to #{schema_file}, it's left unchanged.", level: :info
    end
  end

  def process(line)
    if @exclusions.include?(line.strip)
      comment line
    else
      @lines << line
    end
  end

  def comment(line)
    @lines << "-- The following was commented out by DoppelSchema.create\n"
    @lines << "-- Ensure that this function is available in template1 if it is required\n"
    @lines << '-- ' + line
  end

  def lines_to_strike
    [
      "CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;",
      "COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';",
      "CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;",
      "COMMENT ON EXTENSION hstore IS 'data type for storing sets of (key, value) pairs';"
    ]
  end
end
