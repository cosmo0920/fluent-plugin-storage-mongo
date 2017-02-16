require 'mongo'
require 'fluent/plugin/storage'

module Fluent
  module Plugin
    class MongoStorage < Storage
      Fluent::Plugin.register_storage('mongo', self)

      attr_reader :store # for test

      config_param :path, :string, default: nil
      desc "MongoDB collection"
      config_param :collection, :string, default: "unspecified"
      desc "MongoDB database"
      config_param :database, :string
      desc "MongoDB host"
      config_param :host, :string, default: 'localhost'
      desc "MongoDB port"
      config_param :port, :integer, default: 27017
      desc "MongoDB write_concern"
      config_param :write_concern, :integer, default: nil
      desc "MongoDB journaled"
      config_param :journaled, :bool, default: false
      desc "Replace dot with specified string"
      config_param :replace_dot_in_key_with, :string, default: nil
      desc "Replace dollar with specified string"
      config_param :replace_dollar_in_key_with, :string, default: nil

      # SSL connection
      config_param :ssl, :bool, default: false
      config_param :ssl_cert, :string, default: nil
      config_param :ssl_key, :string, default: nil
      config_param :ssl_key_pass_phrase, :string, default: nil, secret: true
      config_param :ssl_verify, :bool, default: false
      config_param :ssl_ca_cert, :string, default: nil

      attr_reader :client_options, :collection_options

      def initialize
        super

        @client_options = {}
        @collection_options = {capped: false}
        @store = {}
      end

      def configure(conf)
        super

        unless @path
          if conf && !conf.arg.empty?
            @path = conf.arg
          else
            raise Fluent::ConfigError, "path or conf.arg for <storage> is required."
          end
        end

        @client_options[:write] = {j: @journaled}
        @client_options[:write].merge!({w: @write_concern}) unless @write_concern.nil?
        @client_options[:ssl] = @ssl

        if @ssl
          @client_options[:ssl_cert] = @ssl_cert
          @client_options[:ssl_key] = @ssl_key
          @client_options[:ssl_key_pass_phrase] = @ssl_key_pass_phrase
          @client_options[:ssl_verify] = @ssl_verify
          @client_options[:ssl_ca_cert] = @ssl_ca_cert
        end
        @client = client
      end

      def multi_workers_ready?
        true
      end

      def load
        begin
          value = {}
          documents = @client[format_collection_name(@collection)].find(_id: @path)
          if documents.count >= 1
            documents.each do |document|
              value.merge!(document)
            end
          end
          value.delete('_id')
          unless value.is_a?(Hash)
            log.error "broken content for plugin storage (Hash required: ignored)", type: json.class
            log.debug "broken content", content: json_string
            return
          end
          @store = value
        rescue => e
          log.error "failed to load data for plugin storage from mongo", path: @path, error: e
        end
      end

      def save
        operate(format_collection_name(@collection), @store)
      end

      def get(key)
        @store[key.to_s]
      end

      def fetch(key, defval)
        @store.fetch(key.to_s, defval)
      end

      def put(key, value)
        @store[key.to_s] = value
      end

      def delete(key)
        @store.delete(key.to_s)
      end

      def update(key, &block)
        @store[key.to_s] = block.call(@store[key.to_s])
      end

      private

      def client
        @client_options[:database] = @database
        @client_options[:user] = @user if @user
        @client_options[:password] = @password if @password
        Mongo::Client.new(["#{@host}:#{@port}"], @client_options)
      end

      FORMAT_COLLECTION_NAME_RE = /(^\.+)|(\.+$)/

      def format_collection_name(collection_name)
        formatted = collection_name
        formatted = formatted.gsub(FORMAT_COLLECTION_NAME_RE, '')
        formatted = @collection if formatted.size == 0 # set default for nil tag
        formatted
      end

      def format_key(record)
        if @replace_dot_in_key_with
          replace_key_of_hash(record, ".", @replace_dot_in_key_with)
        end
        if @replace_dollar_in_key_with
          replace_key_of_hash(record, /^\$/, @replace_dollar_in_key_with)
        end
        record
      end

      def operate(collection, record)
        begin
          record = format_key(record)

          @client[collection, @collection_options].replace_one({_id: @path}, record, {upsert: true})
        rescue Mongo::Error::BulkWriteError => e
          log.warn "document is not inserted. Maybe this document is invalid as a BSON."
        rescue ArgumentError => e
          log.warn e
        end
        record
      end

      # copied from https://github.com/fluent/fluent-plugin-mongo/blob/c989ae01d21513c8d45b5338431586542aa93b0d/lib/fluent/plugin/out_mongo.rb#L223-L244
      def replace_key_of_hash(hash_or_array, pattern, replacement)
        case hash_or_array
        when Array
          hash_or_array.map do |elm|
            replace_key_of_hash(elm, pattern, replacement)
          end
        when Hash
          result = Hash.new
          hash_or_array.each_pair do |k, v|
            k = k.gsub(pattern, replacement)

            if v.is_a?(Hash) || v.is_a?(Array)
              result[k] = replace_key_of_hash(v, pattern, replacement)
            else
              result[k] = v
            end
          end
          result
        else
          hash_or_array
        end
      end
    end
  end
end
