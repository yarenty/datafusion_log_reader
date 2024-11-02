pub mod context;
pub mod datasource;
pub mod execution_plan;
pub mod reader_options;

use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::{
    array::{RecordBatch, RecordBatchReader},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use chrono::{DateTime, Utc};
use datafusion::common::DataFusionError;
use log::trace;
use std::iter::Iterator;
use std::sync::Arc;
use thiserror::Error;

type Result<T> = std::result::Result<T, DataFusionLogError>;

#[derive(Error, Debug)]
pub enum DataFusionLogError {
    /// datafusion related errors
    #[error("{0}")]
    DataFusion(#[from] DataFusionError),
    /// Arrow related errors
    #[error("{0}")]
    Arrow(#[from] ArrowError),
    /// IO related errors
    #[error("{0}")]
    IO(#[from] std::io::Error),
    /// general errors we don't know where to put
    #[error("{0}")]
    General(String),
    /// deserialization errors
    #[error("{0}")]
    Format(String),
}

#[derive(Debug, Clone)]
pub struct LogReader {
    path: String,
    schema: Schema,
}

impl LogReader {
    pub fn new(path: &str) -> Result<LogReader> {
        //check if path exists
        if !std::path::Path::new(path).exists() {
            return Err(DataFusionLogError::General(format!(
                "path {} does not exist",
                path
            )));
        }

        let schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("level", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]);

        Ok(LogReader {
            path: path.to_string(),
            schema,
        })
    }

    fn read_all(&mut self) -> Result<RecordBatch> {
        let mut time = vec![];
        let mut level = vec![];
        let mut location = vec![];
        let mut message = vec![];

        //TODO: this should be proper configurable pattern match - but not at this point of time
        let binding = std::fs::read_to_string(&self.path)?;
        for line in binding.lines() {
            // for line in std::fs::read_to_string(&self.path)?.lines() {
            trace!("{}", line);
            let tokens: Vec<_> = line
                .split(&['[', ' ', ']'][..])
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|s| !s.is_empty())
                .collect();

            // just temp for test
            let mess = line
                .split_at(line.find(tokens[2]).unwrap() + tokens[2].len() + 2)
                .1;

            trace!("tokens: {:?}", tokens);

            let dt: DateTime<Utc> = tokens[0].parse().unwrap();
            time.push(dt.timestamp_micros());
            level.push(tokens[1]);
            location.push(tokens[2]);
            message.push(mess);
        }

        let time_arr = TimestampMicrosecondArray::from(time);
        let level_arr = StringArray::from(level);
        let location_arr = StringArray::from(location);
        let message_arr = StringArray::from(message);

        let record_batch = RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(time_arr),
                Arc::new(level_arr),
                Arc::new(location_arr),
                Arc::new(message_arr),
            ],
        )
        .unwrap();

        Ok(record_batch) //record_batch
    }

    // TODO: implement for push down query
    // fn read_filetred(&mut self, filter: Option<&str>) -> Result<RecordBatch> {
    //     self.read_all()
    // }
}

impl Iterator for LogReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_all() {
            Ok(record_batch) => Some(Ok(record_batch)),
            Err(e) => Some(Err(ArrowError::CastError(e.to_string()))),
        }
    }
}

impl RecordBatchReader for LogReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }
}

#[cfg(test)]
mod test {

    use crate::LogReader;
    use arrow::array::RecordBatchReader;

    #[test]
    fn should_not_exist() -> super::Result<()> {
        let package_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{package_dir}/data_not_exits.log");

        let reader = LogReader::new(&path);

        assert!(reader.is_err());
        Ok(())
    }

    #[test]
    fn should_have_proper_schema() -> super::Result<()> {
        let package_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{package_dir}/tests/data/test.log");

        let mut reader = LogReader::new(&path)?;

        let _schema = reader.schema();
        let result = reader.read_all()?;

        let schema = "Field { name: \"time\", data_type: Timestamp(Microsecond, None), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"level\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"location\", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"message\", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }";

        assert_eq!(schema.to_string(), result.schema().to_string());


        Ok(())
    }

    #[test]
    fn should_read_all() -> super::Result<()> {
        let package_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{package_dir}/tests/data/test.log");

        let mut reader = LogReader::new(&path)?;

        let _schema = reader.schema();
        let result = reader.read_all()?;

        assert_eq!(4, result.num_columns());
        assert_eq!(15, result.num_rows());

        Ok(())
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder().is_test(true).try_init();
}
