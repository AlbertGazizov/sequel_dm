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
          entity.send("#{mapping.entity_field}=", to_attribute(hash, value, mapping)) if mapping.set_field?
        end
      end
      entity
    end

    def to_hash(entity, *args)
      # if it's insert then map all fields, else only loaded
      hash = {}
      if entity.instance_variable_defined?(:@persistance_state)
        persistance_state = entity.instance_variable_get(:@persistance_state)
        entity_mappings = self.mappings.select { |column, mapping| persistance_state.has_key?(mapping.column_name) }
        hash[:id] = entity.id
        entity_mappings.each do |column, mapping|
          new_column_value = to_column(entity, mapping, *args)
          previous_column_value = persistance_state[column]
          hash[column] = new_column_value if column_value_changed?(previous_column_value, new_column_value)
        end
      else
        entity_mappings = self.mappings
        entity_mappings.each do |column, mapping|
          hash[column] = to_column(entity, mapping, *args)
        end
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

    def column_value_changed?(previous_value, new_value)
      previous_value != new_value
    end

  end

end

