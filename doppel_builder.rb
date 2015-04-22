class DoppelBuilder
  require 'colorize'
  require 'set'

  PERFORMANCE_THRESHOLD = 5

  def initialize(job)
    @doppel_config = DoppelConfig.new(job)
    @user = @doppel_config.authorizing_user
    @dead_count = 0
    @found_count = 0
    @long_running_count = 0
    @wasted_seconds = 0
    @long_run_msgs = []
  end

  def run(instance = nil)
    instances = build_instances(instance)
    instances.each do |inst|
      logit "#{identify_myself} building data structure for #{inst.inspect} on behalf of #{@user.inspect}", level: :info
      recurse_on(inst)
    end
    logit "#{identify_myself} finished building data structures", level: :info
    logit "\n#{identify_myself} #{stats}", level: :debug, color: :blue
  end

  def config
    @doppel_config
  end

  def records
    @doppel_config.records
  end

  private

  def stats
    stats =  "found_count: #{@found_count}\ndead_count: #{@dead_count}\n"
    stats += "long_running_count: #{@long_running_count}\nwasted_seconds: #{@wasted_seconds}\n"
    stats += "long_running_msgs:\n#{long_run_msgs_to_s}"
  end

  def long_run_msgs_to_s
    msgs = ''
    @long_run_msgs.each do |msg|
      msgs += "\t#{msg}\n"
    end
    msgs
  end

  def identify_myself
    @my_name ||= "DoppelBuilder(:#{config.job_name})"
  end

  def blacklisted?(object)
    @doppel_config.blacklisted.include? class_of(object)
  end

  def build_instances(instance)
    if instance
      return [instance] unless instance.respond_to? :each
      return instance
    else
      return reconstitute_instances
    end
  end

  def reconstitute_instances
    instances = []
    @doppel_config.instances.each do |class_name, ids|
      klass = Kernel.const_get(class_name)
      ids.each do |id|
        instances << klass.find(id)
      end
    end
    instances
  end

  def get_ids(instance)
    return if blacklisted? instance
    process_instance(instance)
    keys = compose_association_keys(instance)
    # logit "#{identify_myself} get associations #{keys} from #{identify(instance)}", level: :debug, color: :magenta
    keys.each do |key|
      # logit "#{identify_myself} retrieve #{identify(instance)}.send(:#{key})", level: :debug, color: :magenta
      process_association(instance, key)
    end
  end

  def process_association(instance, key)
    association = nil
    duration = Benchmark.realtime do
      association = instance.send(key)
      association.inspect
    end
    process_instance(association)
    warn_if_long_running(duration, :process_association, "#{identify(instance)}.send(:#{key})")
  end

  def warn_if_long_running(duration, method, examine = nil)
    return nil if duration < PERFORMANCE_THRESHOLD
    long_run_msg = "#{identify_myself} long running :#{method} (#{duration}) seconds #{examine}"
    logit long_run_msg, level: :warn, color: :red
    record_long_run_stats(long_run_msg, duration)
  end

  def record_long_run_stats(msg, duration)
    @long_running_count += 1
    @wasted_seconds += (duration - PERFORMANCE_THRESHOLD)
    @long_run_msgs << msg
  end

  def identify(instance)
    "#{instance.class.name}.find(#{instance.id})"
  end

  def compose_association_keys(object)
    all_keys = object.reflections.keys - blacklisted_associations
    return all_keys unless one_way_associations?
    all_keys - belongs_to_keys(object)
  end

  def one_way_associations?
    true
  end

  def blacklisted_associations
    [:versions]
  end

  def belongs_to_keys(object)
    object.class.reflect_on_all_associations(:belongs_to).collect(&:name)
  end

  def process_instance(object)
    process_as_ar_model(object)
    process_as_iterable(object)
  end

  def process_as_iterable(objects)
    return unless objects.is_a?(ActiveRecord::Relation) || objects.is_a?(Array)
    # logit "#{identify_myself} process_instance #{objects.class.name} of #{objects.first.class.name}, count: #{objects.count}", level: :debug
    objects.each do |object|
      process_instance(object)
    end
  end

  def process_as_ar_model(object)
    return unless object.is_a?(ActiveRecord::Base)
    # logit "#{identify_myself} process_instance #{identify(object)}", level: :debug
    create_key(object)
    record_id(object)
  end

  def create_key(instance)
    records[class_of(instance)] ||= Set.new
  end

  def record_id(instance)
    return if blacklisted? instance
    return if already_recorded? instance
    add_to_record_set instance
    recurse_on instance
  end

  def already_recorded?(instance)
    return false unless records[class_of(instance)].include?(instance.id)
    # logit "#{identify_myself} already_recorded #{identify(instance)}", level: :debug, color: :yellow
    @dead_count += 1
    true
  end

  def add_to_record_set(instance)
    records[class_of(instance)].add(instance.id)
    # logit "#{identify_myself} add_to_record_set #{identify(instance)}", level: :debug, color: :green
    @found_count += 1
    #logit "found_count: #{@found_count}; dead_count: #{@dead_count}; long_running_count: #{@long_running_count}", level: :debug, color: :white
  end

  def recurse_on(instance)
    logit "#{identify_myself} recurse on #{identify(instance)}", level: :info
    get_ids(instance)
  end

  def class_of(object)
    object.class.name
  end
end
