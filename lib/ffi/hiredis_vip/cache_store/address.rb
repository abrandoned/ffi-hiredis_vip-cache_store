module FFI
  module HiredisVip
    module CacheStore
      class Address
        attr_reader :database, :host, :port

        def initialize(address)
          address = address.gsub("redis://", "")
          @host = address.split(":").first
          port_and_db = address.split(":").last
          @port = port_and_db.split("/").first
          @database = port_and_db.split("/").last if port_and_db.include?("/")
        end
      end
    end
  end
end
