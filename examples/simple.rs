use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_log_reader::context::LogSessionContextExt;


#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // register table
    // let _ = ctx.register_table("logs", Arc::new(LogDataSource::new("logs", "./data/test.log")));

    let _ = ctx.register_log("logs", "./tests/data/test.log", None).await;

    let df = ctx.sql("select * from logs where level='INFO' limit 2").await?;

    df.show().await?;

    Ok(())
}
