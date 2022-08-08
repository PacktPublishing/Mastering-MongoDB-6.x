require './crud_client'

class CrudCreate < CrudClient
  def find_one
    result = @collection.find({ isbn: '102', isbn: '101' })
    result.each do |doc|
      puts doc.inspect
    end
  end

  def find_or
    result = @collection.find('$or' => [{ isbn: '101' }, { isbn: '102' }]).to_a
    puts result
  end

  def find_embedded
    result = @collection.find({'meta.authors': 'alex giamas'}).to_a
    puts result
  end
end

puts 'Find one document:'
CrudCreate.new.find_one

puts 'Find with OR operator:'
CrudCreate.new.find_or

puts 'Find using an embedded attribute:'
CrudCreate.new.find_embedded