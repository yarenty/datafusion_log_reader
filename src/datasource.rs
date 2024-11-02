use crate::execution_plan::LogExecutionPlan;
use crate::LogReader;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use log::{info, trace};
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        info!("filters: {:?}", filters);
        info!("limit: {:?}", limit);
        let predicate = self.filters_to_predicate(state, filters)?;

        //https://www.youtube.com/watch?v=iJhRbDFJjbg&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=2


        //     let indexed_file = &self.indexed_file;

        //
        //     // Figure out which row groups to scan based on the predicate
        //     let access_plan = self.create_plan(&predicate)?;
        //     println!("{access_plan:?}");
        //
        //     let partitioned_file = indexed_file
        //         .partitioned_file()
        //         // provide the starting access plan to the ParquetExec by
        //         // storing it as  "extensions" on PartitionedFile
        //         .with_extensions(Arc::new(access_plan) as _);
        //
        //     // Prepare for scanning
        //     let schema = self.schema();
        //     let object_store_url = ObjectStoreUrl::parse("file://")?;
        //     let file_scan_config = FileScanConfig::new(object_store_url, schema)
        //         .with_limit(limit)
        //         .with_projection(projection.cloned())
        //         .with_file(partitioned_file);
        //
        //     // Configure a factory interface to avoid re-reading the metadata for each file
        //     let reader_factory =
        //         CachedParquetFileReaderFactory::new(Arc::clone(&self.object_store))
        //             .with_file(indexed_file);
        //
        //     // Finally, put it all together into a ParquetExec
        //     Ok(ParquetExecBuilder::new(file_scan_config)
        //         // provide the predicate so the ParquetExec can try and prune
        //         // row groups internally
        //         .with_predicate(predicate)
        //         // provide the factory to create parquet reader without re-reading metadata
        //         .with_parquet_file_reader_factory(Arc::new(reader_factory))
        //         .build_arc())

       self.create_physical_plan(projection, self.schema()).await

    }

    /// Tell DataFusion to push filters down to the scan method
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact because the pruning can't handle all expressions and pruning
        // is not done at the row level -- there may be rows in returned files
        // that do not pass the filter
        //TODO: change to Exact
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
