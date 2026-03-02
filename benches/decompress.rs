use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn compress_frame(data: &[u8], level: i32) -> Vec<u8> {
    zstd::bulk::compress(data, level).expect("compression failed")
}

fn make_multi_frame(data: &[u8], chunk_size: usize, level: i32) -> Vec<u8> {
    data.chunks(chunk_size)
        .flat_map(|chunk| compress_frame(chunk, level))
        .collect()
}

fn generate_realistic_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    let mut state: u64 = 0xDEADBEEF;
    for byte in data.iter_mut() {
        // simple xorshift — semi-random but still somewhat compressible
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    data
}

fn bench_decompress(c: &mut Criterion) {
    let sizes = [
        ("10MB", 10 * 1024 * 1024),
        ("100MB", 100 * 1024 * 1024),
        ("500MB", 500 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("decompress");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    for (label, size) in sizes {
        let data = generate_realistic_data(size);
        let single = compress_frame(&data, 3);
        let multi = make_multi_frame(&data, 4 * 1024 * 1024, 3);

        // tells criterion the throughput so it reports MB/s
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("zstd-single", label),
            &single,
            |b, compressed| b.iter(|| zstd::bulk::decompress(compressed, size + 1024).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("pzstd-parallel", label),
            &multi,
            |b, compressed| b.iter(|| pzstd::decompressor::decompress(compressed).unwrap()),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_decompress);
criterion_main!(benches);
