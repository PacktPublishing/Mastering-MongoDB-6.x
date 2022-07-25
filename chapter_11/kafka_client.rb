require 'kafka'
require 'csv'
require 'json'
require './mongo_exchange_client'

class KafkaClient
  def initialize
    @kafka = Kafka.new(
      # At least one of these nodes must be available:
      seed_brokers: ["localhost:9092", "localhost:9092"],
      # Set an optional client id in order to identify the client to Kafka:
      client_id: 'xmr-btc',
    )
  end

  def simple_consumer
    @kafka.each_message(topic: "xmr-btc") do |message|
      puts message.offset, message.key, message.value
    end
  end

  def consume
    consumer = @kafka.consumer(group_id: 'xmr-consumers')
    consumer.subscribe('xmr-btc', start_from_beginning: true)
    trap('TERM') { consumer.stop }

    consumer.each_message(automatically_mark_as_processed: false) do |message|
      puts message.value
      if valid_json?(message.value)
        MongoExchangeClient.new.insert(message.value)
        consumer.mark_message_as_processed(message)
      end
    end
    consumer.stop
  end

  def produce
    options = { converters: :numeric, headers: true }

    CSV.foreach('xmr_btc.csv', options) do |row|
      json_line = JSON.generate(row.to_hash)
      @kafka.deliver_message(json_line, topic: 'xmr-btc')
    end
  end

  private def valid_json?(json)
    JSON.parse(json)
    return true
  rescue JSON::ParserError => e
    return false
  end
end


KafkaClient.new.produce

KafkaClient.new.consume
