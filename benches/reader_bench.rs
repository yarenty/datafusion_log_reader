use arrow::array::RecordBatchReader;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_log_reader::LogReader;

// Bench at the moment just for fun ;-)
fn read_log() -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = LogReader::new("./tests/data/test.log")?;

    let _schema = reader.schema();
    let result = reader.next().unwrap().unwrap();

    assert_eq!(4, result.num_columns());
    assert_eq!(15, result.num_rows());
    Ok(())
}

fn benchmark_log_reader(c: &mut Criterion) {
    c.bench_function("read_log", |b| b.iter(|| read_log()));
}

criterion_group!(benches, benchmark_log_reader);
criterion_main!(benches);
