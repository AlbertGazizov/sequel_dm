require 'sequel_dm/extensions/select_fields'

SequelDM::DAO = Class.new(Sequel::Model)
module SequelDM
  def self.DAO(source)
    Class.new(SequelDM::DAO).set_dataset(source)
  end

  class DAO
    class_attribute :mapper

    dataset_module do
      include SequelDM::Extensions::SelectFields
    end

    class << self
      def def_one_to_many(opts)
        one_to_one = opts[:type] == :one_to_one
        name = opts[:name]
        model = self
        key = (opts[:key] ||= opts.default_key)
        km = opts[:key_method] ||= opts[:key]
        cks = opts[:keys] = Array(key)
        opts[:key_methods] = Array(opts[:key_method])
        primary_key = (opts[:primary_key] ||= self.primary_key)
        opts[:eager_loader_key] = primary_key unless opts.has_key?(:eager_loader_key)
        cpks = opts[:primary_keys] = Array(primary_key)
        pkc = opts[:primary_key_column] ||= primary_key
        pkcs = opts[:primary_key_columns] ||= Array(pkc)
        raise(Error, "mismatched number of keys: #{cks.inspect} vs #{cpks.inspect}") unless cks.length == cpks.length
        uses_cks = opts[:uses_composite_keys] = cks.length > 1
        slice_range = opts.slice_range
        opts[:dataset] ||= proc do
          opts.associated_dataset.where(opts.predicate_keys.zip(cpks.map{|k| send(k)}))
        end
        opts[:eager_loader] = proc do |eo|
          h = eo[:id_map]
          rows = eo[:rows]
          reciprocal = opts.reciprocal
          klass = opts.associated_class
          filter_keys = opts.predicate_key
          ds = model.eager_loading_dataset(opts, klass.where(filter_keys=>h.keys), nil, eo[:associations], eo)
          assign_singular = true if one_to_one
          case opts.eager_limit_strategy
          when :distinct_on
            ds = ds.distinct(*filter_keys).order_prepend(*filter_keys)
          when :window_function
            delete_rn = true
            rn = ds.row_number_column
            ds = apply_window_function_eager_limit_strategy(ds, opts)
          when :ruby
            assign_singular = false if one_to_one && slice_range
          end
          ds.all do |assoc_record|
            assoc_record.values.delete(rn) if delete_rn
            hash_key = uses_cks ? km.map{|k| assoc_record.send(k)} : assoc_record.send(km)
            next unless objects = h[hash_key]
            if assign_singular
              objects.each do |object|
                unless object.send(name)
                  # TODO: add persistance_associations update here
                  object.send("#{name}=", assoc_record)
                  assoc_record.send("#{reciprocal}=", object) if reciprocal
                end
              end
            else
              objects.each do |object|
                add_to_associations_state(object, name, assoc_record)
                object.send(name).push(assoc_record)
                assoc_record.send("#{reciprocal}=", object) if reciprocal
              end
            end
          end
          if opts.eager_limit_strategy == :ruby
            if one_to_one
              if slice_range
                rows.each{|o| o.associations[name] = o.associations[name][slice_range.begin]}
              end
            else
              rows.each{|o| o.associations[name] = o.associations[name][slice_range] || []}
            end
          end
        end
        super
      end

      def set_dataset_row_proc(ds)
        ds.row_proc = Proc.new do |raw|
          raise StandardError, "Mapper should be specified" if !self.mapper
          entity = self.mapper.to_entity(raw)
          save_state(entity, raw)
          entity
        end
      end

      def set_mapper(mapper)
        SequelDM::ArgsValidator.is_class!(mapper, :mapper)
        self.mapper = mapper
      end

      # Database methods

      def insert(entity, root = nil)
        raw = mapper.to_hash(entity, root)
        key = dataset.insert(raw)
        set_entity_primary_key(entity, raw, key)
        save_state(entity, raw)
        insert_associations(entity)
        entity
      end

      def insert_all(entities, root = nil)
        entities.each do |entity|
          insert(entity, root)
        end
      end

      def update(entity, root = nil)
        raw = mapper.to_hash(entity, root)
        raw = select_only_changed_values(entity, raw)

        unless raw.empty?
          update_state(entity, raw)

          key_condition = prepare_key_condition_from_entity(entity)
          dataset.where(key_condition).update(raw)
        end

        insert_or_update_associations(entity)
        entity
      end

      def update_all(entities, root = nil)
        entities.each do |entity|
          update(entity, root)
        end
      end

      def save(entity, root = nil)
        if has_persistance_state?(entity)
          update(entity, root)
        else
          insert(entity, root)
        end
      end

      def save_all(entities, root = nil)
        entities.each do |entity|
          save(entity, root)
        end
      end

      def delete(entity)
        key_condition = prepare_key_condition_from_entity(entity)
        dataset.where(key_condition).delete
        delete_associations(entity)
      end

      # TODO: refactor
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

      def select_only_changed_values(entity, hash)
        changes = {}
        return hash unless entity.instance_variable_defined?(:@persistance_state)

        persistance_state = entity.instance_variable_get(:@persistance_state)
        hash.each do |column, value|
          previous_column_value = persistance_state[column]
          if persistance_state.has_key?(column) && column_value_changed?(previous_column_value, value)
            changes[column] = value
          end
        end
        changes
      end

      def column_value_changed?(previous_value, new_value)
        previous_value != new_value
      end

      def save_state(entity, raw)
        if !entity.is_a?(Integer) && !entity.is_a?(Symbol)
          entity.instance_variable_set(:@persistance_state, raw)
        end
      end

      def update_state(entity, raw)
        persistance_state = entity.instance_variable_get(:@persistance_state)
        if persistance_state
          persistance_state.merge!(raw)
        end
      end

      def has_persistance_state?(entity)
        !!entity.instance_variable_get(:@persistance_state)
      end

      def set_associations_state(entity, association_name, associations)
        persistance_associations = entity.instance_variable_get(:@persistance_associations) || {}
        persistance_associations[association_name] ||= []
        persistance_associations[association_name] |= associations
        entity.instance_variable_set(:@persistance_associations, persistance_associations)
      end

      def add_to_associations_state(entity, association_name, association)
        persistance_associations = entity.instance_variable_get(:@persistance_associations) || {}
        persistance_associations[association_name] ||= []
        persistance_associations[association_name] << association
        entity.instance_variable_set(:@persistance_associations, persistance_associations)
      end

      def prepare_key_condition_from_entity(entity)
        key_condition = {}
        if primary_key.is_a?(Array)
          primary_key.each do |key_part|
            key_part_value = entity.send(key_part)
            raise ArgumentError, "entity's primary key can't be nil, got nil for #{key_part}" unless key_part_value
            key_condition[key_part] = key_part_value
          end
        elsif primary_key.is_a?(Symbol)
          key_value = entity.send(primary_key)
          raise ArgumentError, "entity's primary key can't be nil, got nil for #{primary_key}" unless key_value
          key_condition[primary_key] = key_value
        else
          raise StandardError, "primary key should be array or symbol"
        end
        key_condition
      end

      def set_entity_primary_key(entity, raw, key)
        if key && !primary_key.is_a?(Array)
          entity.send("#{primary_key}=", key)
          raw[primary_key] = key
        end
      end

      def insert_associations(entity)
        unless association_reflections.empty?
          association_reflections.each do |association_name, options|
            association_dao = options[:class]
            if entity.respond_to?(association_name)
              children = association_dao.insert_all(entity.send(association_name), entity)
              set_associations_state(entity, association_name, children)
            end
          end
        end
      end

      def insert_or_update_associations(entity)
        unless association_reflections.empty?
          association_reflections.each do |association_name, options|
            association_dao = options[:class]
            raise ArgumentError, "class option should be specified for #{association_name}" unless association_dao

            delete_dissapeared_children(entity, association_name, options)

            children = entity.send(association_name)
            association_dao.save_all(children, entity)
            set_associations_state(entity, association_name, children)
          end
        end
      end

      def delete_associations(entity)
        unless association_reflections.empty?
          association_reflections.each do |association, options|
            if options[:delete]
              association_dao = options[:class]
              conditions = (options[:conditions] || {}).merge(options[:key] => entity.send(primary_key))
              association_dao.where(conditions).delete
            end
          end
        end
      end

      def delete_dissapeared_children(entity, association, options)
        association_dao = options[:class]
        unless options[:key]
          raise ArgumentError, "key option should be specified for #{association}"
        end
        if options[:key].is_a?(Symbol)
          conditions = (options[:conditions] || {}).merge(options[:key] => entity.send(primary_key))
        elsif options[:key].is_a?(Array)
          conditions = options[:key].inject(options[:conditions] || {}) { |result, key| result[key] = entity.send(key); result }
        else
          raise ArgumentError, "key should be symbol or array"
        end

        # get ids of removed children
        association_objects = get_association_objects(entity, association)
        dissapeared_objects = association_objects - entity.send(association)

        scope_key = options[:scope_key] || association_dao.primary_key
        if scope_key.is_a?(Symbol)
          child_keys = { scope_key => [] }
          dissapeared_objects.each do |child_object|
            key = child_object.send(scope_key)
            child_keys[scope_key] << key
          end

          if !child_keys[scope_key].empty?
            association_dao.where(conditions).where(child_keys).delete
          end
        elsif scope_key.is_a?(Array)
          child_keys = []
          dissapeared_objects.each do |child_object|
            child_keys << scope_key.inject({}) do |condition, key|
              condition[key] = child_object.send(key)
              condition
            end
          end
          if !child_keys.empty?
            child_keys.each { |keys| keys.merge!(conditions) }
            association_dao.where(child_keys).delete
          end
        elsif scope_key.is_a?(Proc)
          child_keys = []
          dissapeared_objects.each do |child_object|
            child_keys << scope_key.call(child_object)
          end
          if !child_keys.empty?
            child_keys.each { |keys| keys.merge!(conditions) }
            association_dao.where(child_keys).delete
          end
        else
          raise StandardError, "scope key should be array or symbol"
        end
      end

      def get_association_objects(entity, association)
        persistance_associations = entity.instance_variable_get(:@persistance_associations)
        if persistance_associations
          persistance_associations[association] || []
        else
          []
        end
      end

    end
  end

end
