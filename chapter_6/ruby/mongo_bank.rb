require 'mongo'

class MongoBank
  def initialize
    @client = Mongo::Client.new([ '127.0.0.1:27017' ], database: :mongo_bank)
    db = @client.database
    @collection = db[:accounts]

    # drop any existing data
    @collection.drop

    @collection.insert_one('collection': 'accounts', 'account_id': '1', 'account_name': 'Alex', 'account_balance':100)
    @collection.insert_one('collection': 'accounts', 'account_id': '2', 'account_name': 'Mary', 'account_balance':50)

    transfer('1', '2', 30)
    transfer('1', '2', 300)
  end

  def transfer(source_account, target_account, value)
    puts "transferring #{value} Hypnotons from #{source_account} to #{target_account}"
    session = @client.start_session

    session.start_transaction(read_concern: { level: :snapshot }, write_concern: { w: :majority })
    @collection.update_one({ account_id: source_account }, { '$inc' => { account_balance: value*(-1)} }, session: session)
    @collection.update_one({ account_id: target_account }, { '$inc' => { account_balance: value} }, session: session)

    source_account_balance = @collection.find({ account_id: source_account }, session: session).first['account_balance']

    if source_account_balance < 0
      session.abort_transaction
    else
      session.commit_transaction
    end
  end

end


MongoBank.new