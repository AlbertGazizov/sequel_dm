# Helper class for arguments validation
module SequelDM::ArgsValidator
  class << self

    # Checks that specifid +obj+ is a symbol
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_symbol!(obj, obj_name)
      unless obj.is_a?(Symbol)
        raise ArgumentError, "#{obj_name} should be a Symbol"
      end
    end

    # Checks that specifid +obj+ is an Array
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_array!(obj, obj_name)
      unless obj.is_a?(Array)
        raise ArgumentError, "#{obj_name} should be an Array"
      end
    end

    # Checks that specifid +obj+ is a Hash
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_hash!(obj, obj_name)
      unless obj.is_a?(Hash)
        raise ArgumentError, "#{obj_name} should be a Hash"
      end
    end

    # Checks that specifid +obj+ is a Class
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_class!(obj, obj_name)
      unless obj.is_a?(Class)
        raise ArgumentError, "#{obj_name} should be a Class"
      end
    end

    # Checks that specifid +obj+ is a Proc
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_proc!(obj, obj_name)
      unless obj.is_a?(Proc)
        raise ArgumentError, "#{obj_name} should be a Proc"
      end
    end

    # Checks that specifid +obj+ is a symbol or Class
    # @param obj some object
    # @param obj_name object's name, used to clarify error causer in exception
    def is_symbol_or_class!(obj, obj_name)
      if !obj.is_a?(Symbol) && !obj.is_a?(Class)
        raise ArgumentError, "#{obj_name} should be a Symbol or Class"
      end
    end

    # Checks that specifid +hash+ has a specified +key+
    # @param hash some hash
    # @param key hash's key
    def has_key!(hash, key)
      unless hash.has_key?(key)
        raise ArgumentError, "#{hash} should has #{key} key"
      end
    end

    # Checks that specified +block+ is given
    # @param block some block
    def block_given!(block)
      unless block
        raise ArgumentError, "Block should be given"
      end
    end
  end
end
