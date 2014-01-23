require 'rubygems'
require 'bundler/setup'
require 'debugger'
require 'sequel'
require 'sequel_dm'

RSpec.configure do |config|
  config.color_enabled = true

  db = Sequel.mock(:fetch=>{:id => 1, :x => 1}, :numrows=>1, :autoid=>proc{|sql| 10})
  def db.schema(*) [[:id, {:primary_key=>true}]] end
  def db.reset() sqls end
  def db.supports_schema_parsing?() true end
  Sequel::Model.db = DB = db
end
