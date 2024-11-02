use datafusion_log_reader::LogReader;

#[cfg(test)]
mod log_reader_test {

    use crate::LogReader;
    use arrow::array::RecordBatchReader;

    #[test]
    fn should_not_exist() {
        let package_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{package_dir}/data_not_exits.log");

        let reader = LogReader::new(&path);

        assert!(reader.is_err());
    }


    #[test]
    fn should_read_all() {
        let package_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{package_dir}/tests/data/test.log");

        let mut reader = LogReader::new(&path).unwrap();

        let _schema = reader.schema();
        let result = reader.next().unwrap().unwrap();

        assert_eq!(4, result.num_columns());
        assert_eq!(15, result.num_rows());
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder().is_test(true).try_init();
}
