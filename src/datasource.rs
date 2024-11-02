use crate::execution_plan::LogExecutionPlan;
use crate::LogReader;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use log::trace;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LogDataSource {
    name: String,
    #[allow(dead_code)]
    path: String,
    pub reader: LogReader,
}

impl LogDataSource {
    pub fn new(table_name: &str, table_path: &str) -> Self {
        let reader = LogReader::new(table_path).expect("LogReader failed");

        Self {
            name: table_name.into(),
            path: table_path.into(),
            reader,
        }
    }
}

#[async_trait]
impl TableProvider for LogDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.reader.schema.clone())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    //TODO: use projection pushdown - filters and limit!!
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, self.schema()).await
    }
}

impl LogDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LogExecutionPlan::new(
            projections,
            schema,
            self.clone(),
        )))
    }
}

impl Drop for LogDataSource {
    fn drop(&mut self) {
        trace!("!! drop table {}", self.name);
    }
}
