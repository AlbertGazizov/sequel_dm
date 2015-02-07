require 'yaml'
require 'spec_helper'

describe SequelDM::Mapper do
  module MapperTest

    class Event
      attr_accessor :name, :description, :when, :settings

      def initialize(attrs = {})
        @name        = attrs[:name]
        @description = attrs[:description]
        @when        = attrs[:when]
        @settings    = attrs[:settings ]
      end
    end

    class EventMapper
      include SequelDM::Mapper

      map MapperTest::Event do
        column :subject, to: :name
        column :description
        column :settings, load: ->(hash) { YAML.load(hash[:settings]) }, dump: ->(event) { YAML.dump(event.settings) }
      end
    end
  end

  describe ".to_entity" do
    it "should build Event instance from hash using mappings" do
      tomorrow = Time.now + 24*60*60
      event = MapperTest::EventMapper.to_entity({
        subject:     "Meet parents",
        description: "I need to meet them",
        when:        tomorrow,
        settings:    "---\n:important: true\n",
        occurrences_number: 2,
      })
      event.should be_instance_of(MapperTest::Event)

      event.name.should        == "Meet parents"
      event.description.should == "I need to meet them"
      event.when.should        == nil # unlisted column
      event.settings.should    == { important: true }
    end
  end

  describe ".to_hash" do
    it "should convert entity to hash" do
      event = MapperTest::Event.new({
        name: "Event",
        description: "Description",
        when: Time.now,
        settings: { important: true },
      })
      MapperTest::EventMapper.to_hash(event).should == {
        description: "Description",
        settings: "---\n:important: true\n",
        subject: "Event"
      }
    end
  end
end

