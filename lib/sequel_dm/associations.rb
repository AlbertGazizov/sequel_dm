module SequelDM::Associations

  # Overrides Sequel::Model.one_to_many method to allow use our mappers
  # NOTE: this should be refactored, cause it's ugly now
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
    opts[:eager_loader] ||= proc do |eo|
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
      if assign_singular
        rows.each{|object| object.send("#{name}=", nil) }
      else
        rows.each{|object| object.send("#{name}=", []) }
      end
      ds.all do |assoc_record|
        assoc_record.values.delete(rn) if delete_rn
        hash_key = uses_cks ? km.map{|k| assoc_record.send(k)} : assoc_record.send(km)
        next unless objects = h[hash_key]
        if assign_singular
          objects.each do |object|
            unless object.send(name)
              object.send("#{name}=", assoc_record)
              assoc_record.send("#{reciprocal}=", object) if reciprocal
            end
          end
        else
          objects.each do |object|
            object.send(name).push(assoc_record)
            assoc_record.send("#{reciprocal}=", object) if reciprocal # wtf?
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

    join_type = opts[:graph_join_type]
    select = opts[:graph_select]
    use_only_conditions = opts.include?(:graph_only_conditions)
    only_conditions = opts[:graph_only_conditions]
    conditions = opts[:graph_conditions]
    opts[:cartesian_product_number] ||= one_to_one ? 0 : 1
    graph_block = opts[:graph_block]
    opts[:eager_grapher] ||= proc do |eo|
      ds = eo[:self]
      ds = ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : cks.zip(pkcs) + conditions, eo.merge(:select=>select, :join_type=>join_type, :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
      # We only load reciprocals for one_to_many associations, as other reciprocals don't make sense
      ds.opts[:eager_graph][:reciprocals][eo[:table_alias]] = opts.reciprocal
      ds
    end

    def_association_dataset_methods(opts)

    ck_nil_hash ={}
    cks.each{|k| ck_nil_hash[k] = nil}

    unless opts[:read_only]
      validate = opts[:validate]

      if one_to_one
        setter = opts[:setter] || proc do |o|
          up_ds = _apply_association_options(opts, opts.associated_dataset.where(cks.zip(cpks.map{|k| send(k)})))
          if o
            up_ds = up_ds.exclude(o.pk_hash) unless o.new?
            cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
          end
          checked_transaction do
            up_ds.update(ck_nil_hash)
            o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save") if o
          end
        end
        association_module_private_def(opts._setter_method, opts, &setter)
        association_module_def(opts.setter_method, opts){|o| set_one_to_one_associated_object(opts, o)}
      else
        adder = opts[:adder] || proc do |o|
          cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
          o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
        end
        association_module_private_def(opts._add_method, opts, &adder)

        remover = opts[:remover] || proc do |o|
          cks.each{|k| o.send(:"#{k}=", nil)}
          o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
        end
        association_module_private_def(opts._remove_method, opts, &remover)

        clearer = opts[:clearer] || proc do
          _apply_association_options(opts, opts.associated_dataset.where(cks.zip(cpks.map{|k| send(k)}))).update(ck_nil_hash)
        end
        association_module_private_def(opts._remove_all_method, opts, &clearer)

        def_add_method(opts)
        def_remove_methods(opts)
      end
    end
  end
end
