require 'mongo'

class CrudClient
  def initialize
    client = Mongo::Client.new([ '127.0.0.1:27017' ], database: :mongo_book)
    db = client.database
    @collection = db[:books]
  end
end
