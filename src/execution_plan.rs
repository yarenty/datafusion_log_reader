use crate::datasource::LogDataSource;
use arrow::datatypes::SchemaRef;
use datafusion::common::{project_schema, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
use log::{info, trace};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LogExecutionPlan {
    db: LogDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl LogExecutionPlan {
    pub fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: LogDataSource) -> Self {
        info!("projections: {:?}", projections);
        let projected_schema = project_schema(&schema, projections).unwrap();
        info!("projected schema: {:?}", projected_schema);
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for LogExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "LogExecutionPlan")
    }
}

impl ExecutionPlan for LogExecutionPlan {
    fn name(&self) -> &'static str {
        "LogExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let db = self.db.clone();
        // FIXME: remove unwrap
        let result = db.reader.clone().read_all().unwrap();
        Ok(Box::pin(MemoryStream::try_new(vec![result], self.schema(), None)?))
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl Drop for LogExecutionPlan {
    fn drop(&mut self) {
        trace!("!! DROPING execution plan");
    }
}
