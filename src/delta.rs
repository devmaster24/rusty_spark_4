use std::sync::Arc;

use chrono::{DateTime, Utc};
use deltalake::{DeltaOps, DeltaTableBuilder, DeltaTableError};
use deltalake::arrow::array::{GenericByteArray, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use deltalake::arrow::datatypes::{DataType, Field, GenericStringType, Schema, TimeUnit};
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::prelude::{CsvReadOptions, SessionContext};
use jsonschema::JSONSchema;
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use serde_json::Value;

static TEST_NAMES: [&str; 3] = ["Jimmy Test", "Jimbo Test", "Joe Bob"];

static DELTA_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ])
});

static JSON_VALIDATOR: Lazy<JSONSchema> = Lazy::new(|| {
    let raw_schema: Value = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Schema that I made",
        "type": "object",
        "examples": [
            {
                "idx": 1,
                "name": "Jimmy",
                "timestamp": "2024-07-04T19:29:28.625617281+00:00"
            }
        ],
        "properties": {
            "idx": {
                "type": "integer"
            },
            "name": {
                "type": "string"
            },
            "timestamp": {
                "type": "string",
                "format": "timestamp"
            }
        },
        "additionalProperties": true
    });
    JSONSchema::compile(&raw_schema).expect("Invalid JSON schema somehow..?")
});


pub async fn load_data_from_kafka(ctx: &SessionContext, data: Vec<Value>) -> Result<DataFrame, DeltaTableError> {
    let mut idx_arr: Vec<i64> = vec![];
    let mut name_arr: Vec<String> = vec![];
    let mut ts_arr: Vec<Option<i64>> = vec![];

    for record in data {
        // TODO - the below
        // let result = JSON_VALIDATOR.validate(&record);
        let _is_error = JSON_VALIDATOR.is_valid(&record);

        // I don't have the time to parse these timestamps...
        let str_ts = record["timestamp"].as_str().unwrap();
        ts_arr.push(Some(DateTime::parse_from_rfc3339(str_ts).unwrap().timestamp_micros()));
        idx_arr.push(record["idx"].as_i64().unwrap());
        name_arr.push(record["name"].as_str().unwrap().into());
    }

    // Might be worth-while to see if there is a better way to do this
    let name_array: GenericByteArray<GenericStringType<i32>> = name_arr.into();
    let batch = RecordBatch::try_new(
        Arc::new(DELTA_SCHEMA.clone()),
        vec![
            Arc::new(Int64Array::from(idx_arr)),
            Arc::new(name_array),
            Arc::new(TimestampMicrosecondArray::from(ts_arr)),
        ],
    )?;

    Ok(ctx.read_batch(batch)?)
}

pub async fn load_delta_table(
    ctx: &SessionContext,
    path: &str,
    tbl_name: &str,
) -> Result<(), DeltaTableError> {
    let tbl = DeltaTableBuilder::from_uri(path).load().await?;
    ctx.register_table(tbl_name, Arc::new(tbl))?;

    Ok(())
}

pub async fn write_to_delta(df: DataFrame, output_dir: &str) -> Result<(), DeltaTableError> {
    let ops = DeltaOps::try_from_uri(output_dir).await?;
    ops.write(df.collect().await?).await?;

    // println!("Perform an optimize?");
    // let mut input = String::new();
    // io::stdin().read_line(&mut input).expect("error: unable to read user input");
    //
    // if input.to_lowercase().trim() == "y" {
    //     let ops = DeltaOps::try_from_uri(output_dir).await?;
    //     ops.optimize().await?;
    //     println!("Successfully optimized!");
    // }

    Ok(())
}

#[allow(dead_code)]
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
