require 'sequel_dm/args_validator'
require 'sequel_dm/mappings_dsl'

module SequelDM::Mapper
  extend ActiveSupport::Concern

  included do
    class_attribute :entity_class, :mappings
  end

  module ClassMethods
    def map(entity_class, &mappings_proc)
      SequelDM::ArgsValidator.is_class!(entity_class, :entity_class)
      self.entity_class = entity_class
      self.mappings     = SequelDM::MappingsDSL.new(&mappings_proc).mappings
    end

    def to_entity(hash)
      attributes = {}
      entity = self.entity_class.new
      hash.each do |key, value|
        if mapping = self.mappings[key]
          entity.instance_variable_set(:"@#{mapping.entity_field}", to_attribute(hash, value, mapping)) if mapping.set_field?
        end
      end
      entity
    end

    def to_hash(entity, *args)
      hash = {}

      entity_mappings = self.mappings
      entity_mappings.each do |column, mapping|
        value = to_column(entity, mapping, *args)
        hash[column] = value if value && mapping.set_column?
      end

      hash
    end

    private

    def to_attribute(hash, value, mapping)
      mapping.load? ? mapping.load(hash) : value
    end

    def to_column(entity, mapping, *args)
      mapping.dump? ? mapping.dump(entity, *args) : entity.send(mapping.entity_field)
    end

  end

end

