use crate::datasource::LogDataSource;
use crate::reader_options::LogReaderOptions;
use crate::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait LogSessionContextExt {
    async fn register_log(
        &self,
        table_name: &str,
        table_path: &str,
        log_read_options: Option<LogReaderOptions<'_>>,
    ) -> Result<Arc<LogDataSource>>;
}

#[async_trait::async_trait]
impl LogSessionContextExt for SessionContext {
    async fn register_log(
        &self,
        table_name: &str,
        table_path: &str,
        _log_read_options: Option<LogReaderOptions<'_>>,
    ) -> Result<Arc<LogDataSource>> {
        let table = LogDataSource::new(table_name, table_path);
        let arc_table = Arc::new(table);

        self.register_table(table_name, arc_table.clone())?;

        Ok(arc_table)
    }
}
