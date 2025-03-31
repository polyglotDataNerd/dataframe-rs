use aws_config::*;
use aws_sdk_s3::*;
use bytes::Bytes;
use polars::prelude::*;
use std::io::Cursor;
pub use tokio::io::*;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = defaults(BehaviorVersion::v2025_01_17())
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let client = Client::new(&config);
    let bucket = "s3-bucket";
    let key = "/blockchain/transfers/erc20";
    let s3_payload: Option<Bytes> = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            let payload = resp
                .body
                .collect()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
                .into_bytes();
            Some(payload)
        }
        Err(e) => {
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };
    if let Some(payload) = s3_payload {
        let cur = Cursor::new(payload);
        let df = ParquetReader::new(cur).finish();
        let show = df.unwrap();
        let fil = show.filter(&show.column("token_value").unwrap().is_not_null());
        println!("{}", fil.unwrap().head(Some(10)));
    }

    Ok(())
}
