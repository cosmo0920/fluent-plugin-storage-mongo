require_relative '../helper'
require 'fluent/test/helpers'
require 'fluent/plugin/storage_mongo'
require 'fluent/plugin/input'

class MongoStorageTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  class MyInput < Fluent::Plugin::Input
    helpers :storage
    config_section :storage do
      config_set_default :@type, 'mongo'
    end
  end

  def collection_name
    'test'
  end

  def database_name
    'fluent_test'
  end

  def port
    27017
  end

  def setup_mongod(collection = collection_name)
    options = {}
    options[:database] = database_name
    @client = ::Mongo::Client.new(["localhost:#{port}"], options)
    @client[collection].drop
  end

  def teardown_mongod(collection = collection_name)
    @client[collection].drop
  end

  setup do
    Fluent::Test.setup
    @d = MyInput.new
    setup_mongod
    @path = 'my_store_key'
  end

  teardown do
    @d.stop unless @d.stopped?
    @d.before_shutdown unless @d.before_shutdown?
    @d.shutdown unless @d.shutdown?
    @d.after_shutdown unless @d.after_shutdown?
    @d.close unless @d.closed?
    @d.terminate unless @d.terminated?
    teardown_mongod
  end

  sub_test_case 'without any configuration' do
    test 'raise Fluent::ConfigError' do
      conf = config_element()

      assert_raise(Fluent::ConfigError) do
        @d.configure(conf)
      end
    end
  end

  sub_test_case '#configure' do
    test "with capped" do
      storage_path = @path
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {
                                                              'path' => storage_path,
                                                              'database' => database_name,
                                                              'collection' => collection_name,
                                                              'capped' => true,
                                                              'capped_size' => 100,
                                                            }
                                                           )])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_true(@p.persistent)

      assert_equal('fluent_test', @p.database)
      assert_equal('test', @p.collection)
      assert_equal('localhost', @p.host)
      assert_equal(port, @p.port)
      assert_equal({capped: true, size: 100}, @p.collection_options)
      assert_equal({database: database_name, ssl: false, write: {j: false}}, @p.client_options)
    end

    test "with write concern" do
      storage_path = @path
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {
                                                              'path' => storage_path,
                                                              'database' => database_name,
                                                              'collection' => collection_name,
                                                              'write_concern' => 2,
                                                            }
                                                           )])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      expected = {
        ssl: false,
        write: {
          j: false,
          w: 2,
        },
      }
      assert_equal('fluent_test', @p.database)
      assert_equal('test', @p.collection)
      assert_equal('localhost', @p.host)
      assert_equal(port, @p.port)
      assert_equal({capped: false}, @p.collection_options)
      assert_equal({database: database_name}.merge(expected), @p.client_options)
    end

    test "with journaled" do
      storage_path = @path
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {
                                                              'path' => storage_path,
                                                              'database' => database_name,
                                                              'collection' => collection_name,
                                                              'journaled' => true,
                                                            }
                                                           )])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      expected = {
        ssl: false,
        write: {
          j: true,
        },
      }
      assert_equal({database: database_name}.merge(expected), @p.client_options)
    end

    test "with mongo_log_level" do
      storage_path = @path
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {
                                                              'path' => storage_path,
                                                              'database' => database_name,
                                                              'collection' => collection_name,
                                                              'mongo_log_level' => 'fatal',
                                                            }
                                                           )])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      assert_equal('fatal', @p.mongo_log_level)
    end
  end

  sub_test_case 'configured with path key' do
    test 'works as storage which stores data into mongo' do
      storage_path = @path
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {
                                                              'path' => storage_path,
                                                              'database' => database_name,
                                                              'collection' => collection_name
                                                            }
                                                           )])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      assert_true(@p.persistent)

      assert_equal storage_path, @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal '2', @p.get('key1')

      @p.save # stores all data into redis

      assert @p.load

      @p.put('key2', 4)

      @d.stop; @d.before_shutdown; @d.shutdown; @d.after_shutdown; @d.close; @d.terminate

      assert_equal({'key1' => '2', 'key2' => 4}, @p.load)

      # re-create to reload storage contents
      @d = MyInput.new
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_false @p.store.empty?

      assert_equal '2', @p.get('key1')
      assert_equal 4, @p.get('key2')
    end
  end

  sub_test_case 'configured with conf.arg' do
    test 'works with customized path key by specified usage' do
      storage_conf = {
        'database' => database_name,
        'collection' => collection_name
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', "#{@path}", storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create(usage: "#{@path}")
      assert_true(@p.persistent)

      assert_equal @path, @p.path
      assert @p.store.empty?

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.save # stores all data into file

      assert_equal({"key1"=>"1"}, @p.load)
    end
  end

  sub_test_case 'configured with replace_dot_in_key_with' do
    test 'works as storage which stores data into mongo' do
      storage_path = @path
      dot_key = '__dot__'
      storage_conf = {
        'path' => storage_path,
        'database' => database_name,
        'collection' => collection_name,
        'replace_dot_in_key_with' => dot_key
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', '', storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      assert_true(@p.persistent)

      assert_equal @path, @p.path
      assert @p.store.empty?

      @p.put('mongo.storage', '1')
      assert_nil @p.get('mongo.storage')

      @p.save # stores all data into file

      assert_equal({"mongo#{dot_key}storage"=>"1"}, @p.load)
    end
  end

  sub_test_case 'configured with replace_dot_in_key_with' do
    test 'works as storage which stores data into mongo' do
      storage_path = @path
      dollar_key = '__dollar__'
      storage_conf = {
        'path' => storage_path,
        'database' => database_name,
        'collection' => collection_name,
        'replace_dollar_in_key_with' => dollar_key
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', '', storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      assert_true(@p.persistent)

      assert_equal @path, @p.path
      assert @p.store.empty?

      @p.put('$storage', '1')
      assert_nil @p.get('$storage')

      @p.save # stores all data into file

      assert_equal({"#{dollar_key}storage"=>"1"}, @p.load)
    end
  end

  sub_test_case 'configured with replace_underbar_id_in_key_with' do
    test 'works as storage which stores data into mongo' do
      storage_path = @path
      underbar_id_key = '__id__'
      storage_conf = {
        'path' => storage_path,
        'database' => database_name,
        'collection' => collection_name,
        'replace_underbar_id_in_key_with' => underbar_id_key
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', '', storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()
      assert_true(@p.persistent)

      assert_equal @path, @p.path
      assert @p.store.empty?

      @p.put('_id', '1')
      assert_nil @p.get('_id')

      @p.save # stores all data into file

      assert_equal({underbar_id_key=>"1"}, @p.load)
    end
  end
end
