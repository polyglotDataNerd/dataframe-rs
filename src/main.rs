mod io {
    pub mod read;
    pub mod write;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = "avalanche-data-platform".to_string();
    let key = "lakehouse/transfers-50276732ab2541e898f2ce3dd3de7055/chain_id=8888/transfer_type=ERC20/block_date=2025-02-14/20250316_172145_16485_7uqww_aa4f8d28-aff9-4fc7-b1ed-9b7ed8ff2478".to_string();
    let local_path = "tests/fixtures/transfers.parquet".to_string();
    let reader = io::read::Reader::new(Some(bucket), Some(key), None, Some(local_path));
    //from s3
    let _s3_payload = reader.s3_reader().await?;
    //from fixture
    let local_parq = reader.local_reader().await?;
    let parq_reader = io::read::ParquetReader::new(local_parq);

    // let parq_reader = io::read::ParquetReader::new(s3_payload);
    let df = parq_reader.parq_reader().await?;
    let fil = df.filter(&df.column("token_value").unwrap().is_not_null());
    println!("{}", fil.unwrap().iter().count());

    Ok(())
}
