use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};
use std::path::Path;

use datafusion::arrow::json;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    // Extract some useful information from the request
    let _who = event
        .query_string_parameters_ref()
        .and_then(|params| params.first("name"))
        .unwrap_or("world");
    let now = Instant::now();

    let mut query_tasks = Vec::new();

    query_tasks.push(tokio::spawn(compute(1)));

    for task in query_tasks {
        let _ = task.await.expect("waiting failed");
    }
    let end = Instant::now();
    tracing::info!("Total Time elapased {:?}", end - now);
    // Return something that implements IntoResponse.
    // It will be serialized to the right response event automatically by the runtime

    let data = fs::read_to_string("/tmp/results1.json").expect("Couldn't read results file");
    // let message = format!("Total Time elapased {:?}", end - now);
    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(data.into())
        .map_err(Box::new)?;
    Ok(resp)
}

async fn compute(id: u16) -> Result<(), DataFusionError> {
    let start = Instant::now();
    // create local session context
    let config = SessionConfig::new();
    let config = config.with_batch_size(2048);

    let ctx = SessionContext::with_config(config);
    let entries = fs::read_dir("/tmp/")?;
    let file_names: Vec<String> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_file() {
                path.file_name()?.to_str().map(|s| s.to_owned())
            } else {
                None
            }
        })
        .collect();

    tracing::info!("All files in temp folder {:?}", file_names);

    tracing::info!(
        "Does file exist src? {}",
        Path::new("/tmp/file1.parquet").is_file()
    );
    // register parquet file with the execution context
    ctx.register_parquet(
        "ph",
        &format!("src/file{}.parquet", id),
        ParquetReadOptions::default(),
    )
    .await?;

    let load_all_data_query = "SELECT * from ph";
    let all_data = ctx.sql(load_all_data_query).await?;

    let table_name = format!("pension_history_{}", id);

    let all_data = all_data.collect().await.unwrap();

    tracing::info!(
        "Registered all data for task {} vec batches length {}",
        id,
        all_data.len()
    );
    let schema = all_data[0].schema(); // Assuming all batches have the same schema

    let table = MemTable::try_new(schema, vec![all_data])?;
    ctx.register_table(&table_name, Arc::new(table))?;

    log::info!("Registered all data to memory for task {}", id);
    let filename = format!("/tmp/results{}.json", id);
    let path = Path::new(&filename);
    let file = fs::File::create(path)?;
    let mut writer = json::LineDelimitedWriter::new(file);
    let mut query_tasks = Vec::new();

    let ctx = Arc::new(ctx);
    let writer = Arc::new(Mutex::new(writer));

    for i in 1..49 {
        let writr = Arc::clone(&writer);
        let ctx = ctx.clone();
        let table_name = table_name.clone();
        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum({}.amount{}) as calc{} FROM {}",
                table_name, i, i, table_name
            );
            let df = ctx.sql(&query).await.expect("Failed to get dataframe");
            // df.collect().await.expect("Failed to show");
            // print the results
            let result = df.collect().await.unwrap();

            let mut writer = writr.lock().unwrap();
            for rec in result {
                writer.write(&rec).expect("Failed to write");
            }
        });
        query_tasks.push(task);
    }

    for i in 1..48 {
        let writr = Arc::clone(&writer);
        let ctx = ctx.clone();
        let table_name = table_name.clone();
        let task = tokio::spawn(async move {
            let query = format!(
                "SELECT sum({}.number{}) as calc{} FROM {}",
                table_name, i, i, table_name
            );
            let df = ctx.sql(&query).await.expect("Failed to get dataframe");
            // df.collect().await.expect("Failed to show");
            let result = df.collect().await.unwrap();

            let mut writer = writr.lock().unwrap();
            for rec in result {
                writer.write(&rec).expect("Failed to write");
            }
        });
        query_tasks.push(task);
    }

    log::info!("Finished pushing all tasks to vec for task {}", id);
    // tokio::join!(query_tasks);
    for task in query_tasks {
        task.await.expect("waiting failed");
    }
    writer.lock().unwrap().finish()?;
    // writer.finish().expect("failed to finish");
    let end = Instant::now();
    // println!("Time elapased {:?}", end - now);
    tracing::info!(
        "Finished executing for task {} in time {:?}",
        id,
        end - start
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
