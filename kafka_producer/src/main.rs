// https://docs.rs/rdkafka/latest/rdkafka/producer/base_producer/struct.BaseProducer.html
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;
use std::env;
use std::string::ToString;
use serde::Serialize;
use chrono::Utc;
use rand::seq::IndexedRandom;
use uuid::Uuid;


static TEST_NAMES: [&str; 3] = [
    "Jimmy Test",
    "Jimbo Test",
    "Joe Bob"
];

#[derive(Serialize)]
struct MockData<'a> {
    idx: i32,
    name: &'a str,
    timestamp: String
}


fn gen_mock_data(index: i32) -> String {
    let a = MockData {
        idx: index,
        name: TEST_NAMES.choose(&mut rand::thread_rng()).unwrap(),
        timestamp: Utc::now().to_rfc3339()
    };

    serde_json::to_string(&a).expect("Failed to serialize!")
}


#[tokio::main]
async fn main() {
    let kafka_server = env::var("KAFKA_SERVER")
        .unwrap_or_else(|_| {
            println!("KAFKA_SERVER not set! Defaulting..");
            "127.0.0.1:9092".into()
        });
    let kafka_topic = env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| {
            println!("KAFKA_TOPIC not set! Defaulting..");
            "testy".into()
        });
    let num_mock_entries = env::var("NUM_MOCK_RECORDS")
        .unwrap_or_else(|_| {
            println!("NUM_MOCK_RECORDS not set! Defaulting..");
            "50".into()
        })
        .parse::<i32>()
        .expect("Failed to parse NUM_MOCK_RECORDS - please provide a valid int32!");

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_server)
        .create()
        .expect("Failed to create kafka producer!");

    for x in 0..num_mock_entries {
        producer.send(
            BaseRecord::to(&kafka_topic)
                .payload(&gen_mock_data(x))
                .key(&Uuid::new_v4().to_string()),
        ).expect("Failed to enqueue");

        if x % 50_000 == 0 {
            producer.flush(Duration::from_secs(10)).expect("Failed to flush Kafka queue!");
            println!("Flush")
        }
    }

    println!("Waiting for all messages to send");
    producer.flush(Duration::from_secs(10)).expect("Failed to flush Kafka queue!");
    println!("All {num_mock_entries} record(s) sent!");
}
