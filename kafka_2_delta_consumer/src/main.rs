// #![allow(unused)]

use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::DeltaTableError;

mod delta;
mod kafka;

const OUTPUT_DIR: &str = "./output";
const NUM_MESSAGES: i32 = 5_000_000;

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    let ctx = SessionContext::new();

    match delta::load_delta_table(&ctx, &OUTPUT_DIR, "local_df").await {
        Ok(_) => {
            let results = ctx.sql("SELECT COUNT(*) FROM local_df").await?.collect().await?;
            println!("Data read in!\n{}", pretty_format_batches(&results)?);
        },
        Err(e) => println!("No data to read in! Error {e}")
    };

    println!("Reading data.");
    let kafka_payload = kafka::consume_kafka_data(&NUM_MESSAGES).await;

    println!("Loading delta table");
    let kafka_df = delta::load_data_from_kafka(&ctx, kafka_payload).await?;

    println!("Writing table");
    delta::write_to_delta(kafka_df, &OUTPUT_DIR).await?;
    Ok(())
}
