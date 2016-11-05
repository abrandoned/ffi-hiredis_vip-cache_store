module FFI
  module HiredisVip
    module CacheStore
      class Address
        attr_reader :database, :host, :namespace, :port

        def initialize(address)
          address = address.gsub("redis://", "")
          @host = address.split(":").first
          port_and_db = address.split(":").last
          port_db_and_namespace = port_and_db.split("/")
          @port = port_db_and_namespace.shift unless port_db_and_namespace.empty?
          @database = port_db_and_namespace.shift unless port_db_and_namespace.empty?
          @namespace = port_db_and_namespace.shift unless port_db_and_namespace.empty?
        end
      end
    end
  end
end
