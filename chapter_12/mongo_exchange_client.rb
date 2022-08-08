require 'mongo'
require 'json'
require 'date'

class MongoExchangeClient
  def initialize
    @collection = Mongo::Client.new([ '127.0.0.1:27017' ], database: :exchange_data).database[:xmr_btc]
  end

  def insert(document)
    document = JSON.parse(document)
    document['createdAt'] = Time.now

    @collection.insert_one(document)
  end
end
