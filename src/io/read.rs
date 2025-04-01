use std::io::Cursor;
use aws_config::*;
use aws_sdk_s3::*;
use bytes::Bytes;
use polars::prelude::ParquetReader as PolarsParquetReader;
use tokio::io::*;
use polars::prelude::*;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

pub struct Reader {
    bucket: Option<String>,
    key: Option<String>,
    region: Option<String>,
    local_path: Option<String>,
}

pub struct ParquetReader {
    payload: Bytes,
}

impl Reader {
    pub fn new(bucket: Option<String>, key: Option<String>, region:Option<String>, local_path: Option<String>) -> Self {
        Self {
            bucket,
            key,
            region,
            local_path,
        }
    }

    pub async fn s3_reader(&self) -> Result<Bytes> {
        let local_region = self.region.as_deref().unwrap_or("us-east-1").to_string();
        let local_bucket = self.bucket.as_deref().unwrap().to_string();
        let local_key = self.key.as_deref().unwrap().to_string();

        let config = defaults(BehaviorVersion::v2025_01_17())
            .region(Region::new(local_region))
            .load()
            .await;
        let client = Client::new(&config);
        let response = client.get_object().bucket(local_bucket).key(local_key).send().await;
        let payload = response.unwrap().body.collect().await?.into_bytes();
        Ok(payload)
    }

    pub async fn local_reader(&self) -> Result<Bytes> {
        let local_path = self.local_path.as_deref().unwrap().to_string();
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(local_path);

        let mut file = File::open(path)?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff)?;
        Ok(Bytes::from(buff))
    }
}

impl ParquetReader {
    pub fn new(payload: Bytes) -> Self {
        Self { payload }
    }
    pub async fn parq_reader(&self) -> PolarsResult<DataFrame> {
        let local_payload = self.payload.clone();
        let cur = Cursor::new(local_payload);
        let df = PolarsParquetReader::new(cur).finish()?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn parq_parse_count() {
        let local_path = "tests/fixtures/transfers.parquet".to_string();
        let reader = Reader::new(None, None, None, Some(local_path));
        let local_parq =  reader.local_reader().await.unwrap();
        let parq_reader = ParquetReader::new(local_parq);
        let df = parq_reader.parq_reader().await.unwrap();
        let fil = df.filter(&df.column("token_value").unwrap().is_not_null());
        assert_eq!(fil.unwrap().iter().count(), 13);
    }
}