require './crud_client'

class CrudCreate < CrudClient
  def create_one
    document = { isbn: '101', name: 'Mastering MongoDB', price: 30}
    result = @collection.insert_one(document)
    puts result.inspect
  end

  def create_many
    documents = [ { isbn: '102', name: 'MongoDB in 7 years', price: 50 },
                  { isbn: '103', name: 'MongoDB for experts', price: 40 } ]
    result = @collection.insert_many(documents)
    puts result.inspect
  end

  def create_many_bulk
    documents = [ { isbn: '102', name: 'MongoDB in 7 years', price: 50 },
                  { isbn: '103', name: 'MongoDB for experts', price: 40 } ]
    @collection.bulk_write([ { insert_one: documents.shift }, { insert_one: documents.shift } ],
                           ordered: true)
  end

end

puts 'Creating one document:'
CrudCreate.new.create_one

puts 'Creating many documents:'
CrudCreate.new.create_many

puts 'Creating many documents with bulk_write:'
CrudCreate.new.create_many_bulk