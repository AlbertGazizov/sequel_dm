require 'spec_helper'
require 'sequel_dm'

describe "SequelDM::DAO" do
  class Entity
  end

  class Mapper
    def self.to_entity(hash)
      Entity.new
    end
  end

  describe ".dataset.row_proc" do
    it "should return entity" do
      dao = Class.new(SequelDM::DAO(:items))
      dao.mapper = Mapper
      dao.dataset.row_proc.call({}).should be_instance_of(Entity)
    end
  end
end
