use chrono::Utc;
use deltalake::arrow::array::{GenericByteArray, Int32Array, TimestampMicrosecondArray};
use deltalake::arrow::datatypes::{DataType, Field, GenericStringType, Schema, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext};
use deltalake::{DeltaOps, DeltaTableBuilder, DeltaTableError};
use rand::seq::SliceRandom;
use std::sync::Arc;

static TEST_NAMES: [&str; 3] = ["Jimmy Test", "Jimbo Test", "Joe Bob"];
const OUTPUT_DIR: &str = "./output";

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    let ctx = SessionContext::new();

    load_delta_table(&ctx, &OUTPUT_DIR, "local_df").await?;
    let results = ctx.sql("SELECT * FROM local_df").await?.collect().await?;
    println!("Data read in!\n{}", pretty_format_batches(&results)?);

    let magic = load_data_from_thin_air(&ctx, 5).await?;
    write_to_delta(magic, &OUTPUT_DIR).await?;

    Ok(())
}

async fn load_data_from_thin_air(
    ctx: &SessionContext,
    num_records: i32,
) -> Result<DataFrame, DeltaTableError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]);

    let mut ids: Vec<i32> = vec![];
    let mut names: Vec<&str> = vec![];
    let mut times: Vec<Option<i64>> = vec![];

    for x in 0..num_records {
        ids.push(x);
        names.push(TEST_NAMES.choose(&mut rand::thread_rng()).unwrap());
        times.push(Some(Utc::now().timestamp_micros()));
    }

    let id_array = Int32Array::from(ids);
    let name_array: GenericByteArray<GenericStringType<i32>> = names.into();
    let ts_array = TimestampMicrosecondArray::from(times);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array), Arc::new(name_array), Arc::new(ts_array)],
    )?;

    Ok(ctx.read_batch(batch)?)
}

#[allow(dead_code)]
async fn load_data_from_csv(
    ctx: &SessionContext,
    file_name: &str,
) -> Result<DataFrame, DeltaTableError> {
    let df = ctx.read_csv(file_name, CsvReadOptions::new()).await?;

    Ok(df)
}

async fn load_delta_table(
    ctx: &SessionContext,
    path: &str,
    tbl_name: &str,
) -> Result<(), DeltaTableError> {
    let tbl = DeltaTableBuilder::from_uri(path).load().await?;
    ctx.register_table(tbl_name, Arc::new(tbl))?;

    Ok(())
}

async fn write_to_delta(df: DataFrame, output_dir: &str) -> Result<(), DeltaTableError> {
    let ops = DeltaOps::try_from_uri(output_dir).await?;
    ops.write(df.collect().await?).await?;

    Ok(())
}
