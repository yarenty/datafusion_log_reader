use arrow::datatypes::{DataType, Schema};

#[derive(Clone)]
pub struct LogReaderOptions<'a> {
    /// The data source schema.
    pub schema: Option<&'a Schema>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::LOG.get_ext().as_str()`.
    pub file_extension: &'a str,

    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,

    pub filer_level: &'a str,
}

impl<'a> Default for LogReaderOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: ".log",
            table_partition_cols: vec![],
            filer_level: "info",
        }
    }
}

impl<'a> LogReaderOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(mut self, table_partition_cols: Vec<(String, DataType)>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Specify schema
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}
