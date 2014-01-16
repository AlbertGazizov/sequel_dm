class SequelDM::MappingsDSL
  attr_reader :mappings
  def initialize(&dsl_block)
    @mappings = {}
    instance_exec(&dsl_block)
  end

  def column(column_name, options = {})
    SequelDM::ArgsValidator.is_symbol!(column_name, :column_name)
    SequelDM::ArgsValidator.is_hash!(options, :column_options)
    SequelDM::ArgsValidator.is_symbol!(options[:to], :to) if options[:to]
    SequelDM::ArgsValidator.is_proc!(options[:load], :load) if options[:load]
    SequelDM::ArgsValidator.is_proc!(options[:dump], :dump) if options[:dump]

    set_field  = options[:set_field] == false ? false : true
    set_column = options[:set_column] == false ? false : true
    mappings[column_name] = Mapping.new(
      column_name,
      options[:to] || column_name,
      options[:load],
      options[:dump],
      set_field,
      set_column,
    )
  end

  def columns(*column_names)
    SequelDM::ArgsValidator.is_array!(column_names, :column_names)
    column_names.each { |column_name| column(column_name) }
  end

  class Mapping
    attr_accessor :column_name, :entity_field, :load, :dump

    def initialize(column_name, entity_field, load = nil, dump = nil, set_field = true, set_column = true)
      @column_name   = column_name
      @entity_field  = entity_field
      @load          = load
      @dump          = dump
      @set_field     = set_field
      @set_column    = set_column
    end

    def set_field?
      @set_field
    end

    def set_column?
      @set_column
    end

    def load?
      !!@load
    end

    def dump?
      !!@dump
    end

    def load(value)
      @load.call(value)
    end

    def dump(value, *args)
      @dump.call(value, *args)
    end
  end
end

