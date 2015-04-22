class DoppelRestore
  require 'digest'
  require 'highline/import'

  def initialize(job)
    @doppel_config = DoppelConfig.new(job)
    @tmp_database_name = tmp_database_name
    create_user unless user_exists?
  end

  def run
    check_files_exist
    prepare_tmp_db
    replace_target_db
  end

  def change_password
    pwd = md5_encrypted_password(password)
    execute_command 'postgres', "ALTER USER #{@doppel_config.username} WITH PASSWORD '#{pwd}'"
  end

  def config
    @doppel_config
  end

  def create_user
    pwd = md5_encrypted_password(password)
    execute_command 'postgres', "CREATE USER #{@doppel_config.username} WITH PASSWORD '#{pwd}'"
  end

  private

  def check_files_exist
    fail "#{doppel_file} does not exist" unless File.exist?(doppel_file)
    fail "#{schema_file} does not exist" unless File.exist?(schema_file)
  end

  def user_exists?
    result = execute_command 'postgres', "SELECT 'true' FROM pg_roles WHERE rolname='#{@doppel_config.username}'"
    result.include? 'true'
  end

  def kill_it
    kill_all_db_connections @tmp_database_name
  end

  def password
    first = ''
    second = nil
    while first != second
      puts "Passwords did not match" if second
      first = ask("Enter password for user '#{@doppel_config.username}':  ") { |q| q.echo = "*" }
      second = ask("Password Confirmation:  ") { |q| q.echo = "*" }
    end
    first
  end

  def md5_encrypted_password(pwd)
    hash = Digest::MD5.new
    hash << pwd + @doppel_config.username
    "md5#{hash.hexdigest}"
  end

  def prepare_tmp_db
    Rails.logger.info "Preparing database #{@tmp_database_name}"
    kill_all_db_connections @tmp_database_name
    drop_database @tmp_database_name
    create_database @tmp_database_name
    apply_schema @tmp_database_name
    load_doppel_sql @tmp_database_name
  end

  def replace_target_db
    Rails.logger.info "Preparing to promote #{@tmp_database_name} to #{@target_database_name}"
    kill_all_db_connections target_database_name
    drop_database target_database_name
    rename_database_tmp_db
  end

  def rename_database_tmp_db
    Rails.logger.info "Rename #{@tmp_database_name} to #{target_database_name}"
    execute_command 'postgres', "ALTER DATABASE #{@tmp_database_name} RENAME TO #{target_database_name}"
  end

  def kill_all_db_connections(db_name)
    Rails.logger.info "Killing all connections to #{db_name}"
    execute_command 'postgres', "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '#{db_name}' AND pid <> pg_backend_pid();"
  end

  def drop_database(db_name)
    Rails.logger.info "Dropping database #{db_name}"
    execute_command 'postgres', "DROP DATABASE #{db_name}"
  end

  def load_doppel_sql(db_name)
    Rails.logger.info "Preparing to restore #{doppel_file} to #{tmp_database_name}"
    execute_file(db_name, doppel_file)
  end

  def apply_schema(db_name)
    Rails.logger.info "Applying schema to #{db_name}"
    execute_file(db_name, schema_file)
  end

  def execute_file(db_name, file)
    execute %(psql -h #{remote_host} -d #{db_name} -U root -f #{file})
  end

  def execute_command(db_name, cmd)
    execute %(psql -h #{remote_host} -d #{db_name} -U root -c "#{cmd}")
  end

  def execute(cmd)
    Rails.logger.info cmd
    `#{cmd}`
  end

  def create_database(db_name)
    Rails.logger.info "Creating database #{db_name}"
    execute_command 'postgres', "CREATE DATABASE #{db_name}"
  end

  def tmp_database_name
    "#{@doppel_config.target_database}_tmp"
  end

  def target_database_name
    @doppel_config.target_database
  end

  def remote_host
    'your.database.here.com'
  end

  def schema_file
    @doppel_config.doppel_schema_name
  end

  def doppel_file
    @doppel_config.doppel_file_name
  end
end
