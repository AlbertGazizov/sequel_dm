require 'sequel_dm/associations'
require 'sequel'

SequelDM::DAO = Class.new(Sequel::Model)
module SequelDM
  def self.DAO(source)
    Class.new(SequelDM::DAO).set_dataset(source)
  end

  class DAO
    extend SequelDM::Associations
    class_attribute :mapper

    dataset_module do
      def select_fields(fields)
        return if fields.empty?
        eager_associations = {}
        fields.each do |association, columns|
          next if association == :fields
          if columns && !columns.is_a?(Array)
            columns = get_columns_from_mapper(association)
          end
          if columns
            table_name = model.association_reflections[association][:class].table_name
            columns = columns.map { |column| :"#{table_name}__#{column}___#{column}" }
            eager_associations[association] = proc{|ds| ds.select(*columns) }
          end
        end

        if fields[:fields].is_a?(Array)
          columns = fields[:fields]
        else
          columns = model.mapper.mappings.keys
        end
        columns = columns.map { |column| :"#{model.table_name}__#{column}___#{column}" }
        eager(eager_associations).select(*columns)
      end

      private

      def get_columns_from_mapper(association)
        reflection = model.association_reflections[association]
        raise ArgumentError, "association with name #{association} is not defined in dao" unless reflection
        association_dao = reflection[:class]
        raise ArgumentError, "association #{association} should have class option" unless association_dao
        association_dao.mapper.mappings.keys
      end
    end

    class << self
      def set_mapper(mapper)
        Utils::ArgsValidator.is_symbol_or_class!(mapper, :mapper)
        unless mapper.is_a?(Class)
          # e.g. Database::Mappers::EventMapper
          mapper = Database::Mappers.const_get(mapper.to_s.camelize, false)
        end
        self.dataset.row_proc = Proc.new do |hash|
          entity = mapper.to_entity(hash)
          entity.instance_variable_set(:@persistance_state, hash)
          entity
        end
        self.mapper = mapper
      end

      # Database methods

      def insert(entity, root = nil)
        raw = mapper.to_hash(entity, root)
        key = dataset.insert(raw)
        if key != 0
          if primary_key.is_a?(Array)
            primary_key.each do |primary_key_part|
              entity.send("#{primary_key_part}=", key)
              raw[primary_key_part] = key
            end
          else
            entity.send("#{primary_key}=", key)
            raw[primary_key] = key
          end
        end
        entity.instance_variable_set(:@persistance_state, raw)
        unless association_reflections.empty?
          association_reflections.each do |association, options|
            association_dao = options[:class]
            association_dao.insert_all(entity.send(association), entity)
          end
        end
        entity
      end

      def insert_all(entities, root = nil)
        entities.each do |entity|
          insert(entity, root)
        end
      end

      def update(entity, root = nil)
        raw = mapper.to_hash(entity, root)
        entity.instance_variable_get(:@persistance_state).merge!(raw)
        key_condition = {}
        if primary_key.is_a?(Array)
          primary_key.each do |key_part|
            key_part_value = raw.delete(key_part)
            raise ArgumentError, "entity's primary key can't be nil, got nil for #{key_part}" unless key_part_value
            key_condition[key_part] = key_part_value
          end
        else
          key_value = raw.delete(primary_key)
          raise ArgumentError, "entity's primary key can't be nil, got nil for #{primary_key}" unless key_value
          key_condition[primary_key] = key_value
        end
        dataset.where(key_condition).update(raw) unless raw.empty?

        unless association_reflections.empty?
          association_reflections.each do |association, options|
            association_dao = options[:class]
            children = entity.send(association)
            conditions = (options[:conditions] || {}).merge(options[:key] => entity.send(primary_key))
            association_dao.where(conditions).exclude(association_dao.primary_key => children.map(&association_dao.primary_key)).delete
            association_dao.save_all(children, entity)
          end
        end

        entity
      end

      def update_all(entities, root = nil)
        entities.each do |entity|
          update(entity, root)
        end
      end

      # @todod some entities use different than id primary key
      def save(entity, root = nil)
        if primary_key.is_a?(Array)
          persisted = primary_key.all? { |key_part| entity.send(key_part) }
        else
          persisted = entity.send(primary_key)
        end
        persisted ? update(entity, root) : insert(entity, root)
      end

      def save_all(entities, root = nil)
        entities.each do |entity|
          save(entity, root)
        end
      end

      def delete(entity)
        key_condition = prepare_key_condition(entity)
        dataset.where(key_condition).delete
        unless association_reflections.empty?
          association_reflections.each do |association, options|
            association_dao = options[:class]
            conditions = (options[:conditions] || {}).merge(options[:key] => entity.send(primary_key))
            association_dao.where(conditions).delete
          end
        end
      end

      def delete_all(entities)
        entity_ids = entities.map(&:id)
        dataset.where(id: entity_ids).delete
        unless association_reflections.empty?
          association_reflections.each do |association, options|
            association_dao = options[:class]
            conditions = (options[:conditions] || {}).merge(options[:key] => entity_ids)
            association_dao.where(conditions).delete
          end
        end
      end

      private

      def prepare_key_condition(entity)
        key_condition = {}
        if primary_key.is_a?(Array)
          primary_key.each do |key_part|
            key_part_value = entity.send(key_part)
            raise ArgumentError, "entity's primary key can't be nil, got nil for #{key_part}" unless key_part_value
            key_condition[key_part] = key_part_value
          end
        else
          key_value = entity.send(primary_key)
          raise ArgumentError, "entity's primary key can't be nil, got nil for #{primary_key}" unless key_value
          key_condition[primary_key] = key_value
        end
        key_condition
      end
    end
  end

end
