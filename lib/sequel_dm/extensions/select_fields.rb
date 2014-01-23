module SequelDM
  module Extensions
    module SelectFields
      def select_fields(fields)
        if fields.empty?
          if !model.association_reflections.empty?
            eager(model.association_reflections.keys)
          else
            self
          end
        else
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
  end
end
