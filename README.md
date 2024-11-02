# Datafusion LOG Reader


## Overview

This crate provides a simple extension to process LOG files



## Example

```rust
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_log_reader::context::LogSessionContextExt;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_log("logs", "./tests/data/test.log", None).await;

    let df = ctx.sql("select * from logs where level='INFO' limit 2").await?;
    df.show().await?;
    
    Ok(())
}

```


Output:

```log
+---------------------+-------+--------------------+----------------------+
| time                | level | location           | message              |
+---------------------+-------+--------------------+----------------------+
| 2024-10-19T20:41:31 | INFO  | datafusion_recipes | INFO: Hello, world!  |
| 2024-10-19T20:42:07 | INFO  | datafusion_recipes | INFO2: Hello, world! |
+---------------------+-------+--------------------+----------------------+
```



## Beginning: Quest

is it possible to create simple file datasource in 1 hour

- table provider
- context extension
- simple file extension
- schema: time | severity | file | message


