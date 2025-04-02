use polars::prelude::*;
use polars::prelude::{CsvWriter as PolarsCsvWriter, ParquetWriter as PolarsParqWriter};
use std::fs::File;

pub struct Writer {
    data_frame: Option<DataFrame>,
    write_type: Option<String>,
    local_path: Option<String>,
}

impl Writer {
    pub fn new(
        data_frame: Option<DataFrame>,
        write_type: Option<String>,
        local_path: Option<String>,
    ) -> Self {
        Self {
            data_frame,
            write_type,
            local_path,
        }
    }

    // write types CSV, PARQUET
    pub async fn writer(&self) -> PolarsResult<()> {
        let local_write_type = self.write_type.as_deref().unwrap().to_string();
        let mut local_dataframe = self.data_frame.as_ref().unwrap().clone();

        if local_write_type == "PARQUET" {
            let local_path = self.local_path.as_deref().unwrap().to_string();
            let file = File::create(local_path)?;
            // let mut buff = Vec::new();
            // let cursor = Cursor::new(&mut buff);
            PolarsParqWriter::new(file).finish(&mut local_dataframe)?;
        }

        if local_write_type == "CSV" {
            let local_path = self.local_path.as_deref().unwrap().to_string();
            let file = File::create(local_path)?;
            // let mut buff = Vec::new();
            // let cursor = Cursor::new(&mut buff);
            PolarsCsvWriter::new(file).finish(&mut local_dataframe)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::io;
    use crate::io::read::CSVReader;
    use io::read::{ParquetReader, Reader};
    use tokio;

    #[tokio::test]
    async fn test_parq_writer_success() {
        let local_source = "tests/fixtures/transfers.parquet".to_string();
        let local_path = "/var/tmp/test.parquet";
        // read
        let reader = Reader::new(None, None, None, Some(local_source));
        let local_parq = reader.local_reader().await.unwrap();
        let parquet_reader = ParquetReader::new(local_parq);
        let df_read = parquet_reader.parq_reader().await.unwrap();

        //write
        let write_parq = Writer::new(
            Some(df_read),
            Some("PARQUET".to_string()),
            Some(local_path.to_string()),
        );
        write_parq.writer().await.unwrap();

        // test
        let reader_test = Reader::new(None, None, None, Some(local_path.to_string()));
        let local_parq_test = reader_test.local_reader().await.unwrap();
        let parquet_reader_test = ParquetReader::new(local_parq_test);
        let df = parquet_reader_test.parq_reader().await.unwrap();
        debug_assert_eq!(df.height(), 55);
    }

    #[tokio::test]
    async fn test_csv_writer_success() {
        let local_source = "tests/fixtures/transfers.csv".to_string();
        let local_path = "/var/tmp/test.csv";
        // read
        let reader = Reader::new(None, None, None, Some(local_source));
        let local_parq = reader.local_reader().await.unwrap();
        let csv_reader = CSVReader::new(local_parq);
        let df_read = csv_reader.csv_reader().await.unwrap();

        //write
        let write_csv = Writer::new(
            Some(df_read),
            Some("CSV".to_string()),
            Some(local_path.to_string()),
        );
        write_csv.writer().await.unwrap();

        // test
        let reader_test = Reader::new(None, None, None, Some(local_path.to_string()));
        let local_csv_test = reader_test.local_reader().await.unwrap();
        let csv_reader_test = CSVReader::new(local_csv_test);
        let df = csv_reader_test.csv_reader().await.unwrap();
        debug_assert_eq!(df.height(), 55);
    }
}
