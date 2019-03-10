
#[macro_use]
extern crate criterion;
extern crate rand;
extern crate appendix;

use criterion::{Criterion, Benchmark, Throughput};
use tempfile::tempdir;

use appendix::Index;

fn appendix_simple_insert(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let index = Index::new(&dir).unwrap();
    c.bench_function("appendix simple insert", move |b| b.iter(|| {
        index.insert(0, 0).unwrap();
    }));
}

fn appendix_throughput(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let index = Index::new(&dir).unwrap();
    let bytes = [0u8; 16];
    let mut i = 0;
    c.bench("insert throughput",
            Benchmark::new("insert throughput",move |b| b.iter(|| {
                i += 1;
                index.insert(i, bytes).unwrap();
            }),
        ).throughput(Throughput::Bytes(bytes.len() as u32)),
    );
}

criterion_group!(benches, appendix_simple_insert, appendix_throughput);
criterion_main!(benches);
