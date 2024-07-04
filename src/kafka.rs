use rdkafka::{ClientConfig, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use serde_json::Value;

pub async fn consume_kafka_data(msgs_to_read: &i32) -> Vec<Value> {
    let mut output: Vec<Value> = vec![];
    let mut offsets: Vec<String> = vec![];

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "test_consumer")
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.commit.interval.ms", "5000")
        .set("enable.auto.offset.store", "false")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["testy"])
        .expect("Can't subscribe to specified topics");

    for _ in 0..*msgs_to_read {
        match consumer.recv().await {
            Err(e) => {
                println!("Kafka error: {}", e);
                continue;
            }
            Ok(m) => {
                // Parse msg to string
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                // Debugging, to remove
                offsets.push(format!("Partition: {}, Offset: {}", m.partition(), m.offset()));

                // Tell Kafka we've processed the message
                consumer.store_offset_from_message(&m).unwrap();

                // Save the data
                let json_data: Value = serde_json::from_str(payload).unwrap_or_default();
                output.push(json_data);
            }
        };
    }

    output
}