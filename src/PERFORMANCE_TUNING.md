# Performance Optimization Guide for AMD EPYC 7763

## Hardware Configuration
- **2x AMD EPYC 7763** (128 cores total)
- **512 GB DDR4** RAM
- **4 NUMA domains per socket**
- **204.8 GB/s memory bandwidth per CPU**

## Configuration Options

### 1. Conservative (Current - chunked config)
```yaml
chunk_size: 14          # 2 weeks of daily files
max_concurrent_chunks: 8 # 8 chunks simultaneously
```
- **Memory usage**: ~107 GB (8 × 14 × 96 files × ~10MB each)
- **Parallelism**: 8 × 14 = 112 daily file pairs processing
- **Expected time**: ~6-8 hours for full year

### 2. Aggressive (Maximum throughput)
```yaml
chunk_size: 21           # 3 weeks of daily files
max_concurrent_chunks: 12 # 12 chunks simultaneously
```
- **Memory usage**: ~241 GB (12 × 21 × 96 files × ~10MB each)
- **Parallelism**: 12 × 21 = 252 daily file pairs processing
- **Expected time**: ~3-4 hours for full year

### 3. Ultra-aggressive (Use with caution)
```yaml
chunk_size: 30           # ~1 month of daily files
max_concurrent_chunks: 16 # 16 chunks simultaneously
```
- **Memory usage**: ~460 GB (16 × 30 × 96 files × ~10MB each)
- **Parallelism**: 16 × 30 = 480 daily file pairs processing
- **Expected time**: ~2-3 hours for full year
- **Risk**: May exhaust memory or overwhelm I/O

## Dask Worker Configuration

The code now automatically optimizes worker configuration:
- **64 workers** × **2 threads each** = 128 total cores
- **~8GB RAM per worker** (512GB ÷ 64 workers)
- **Process-based workers** for better isolation

## Performance Monitoring

### 1. Monitor memory usage:
```bash
# Watch memory consumption
watch -n 5 'free -h && ps aux | grep python | wc -l'
```

### 2. Monitor I/O:
```bash
# Watch I/O activity
iostat -x 5
```

### 3. Monitor Dask dashboard:
- Access at `http://your-node:8787`
- Watch task queue, memory usage, and worker status

## Troubleshooting Performance Issues

### If processing is slow:
1. **Check I/O bottleneck**: Monitor disk utilization with `iostat`
2. **Check memory pressure**: Monitor with `free -h` and `vmstat`
3. **Check network**: If input/output on networked storage
4. **Increase chunk_size**: Reduce overhead from chunking

### If running out of memory:
1. **Reduce max_concurrent_chunks**: Fewer simultaneous chunks
2. **Reduce chunk_size**: Smaller chunks use less memory
3. **Check for memory leaks**: Monitor per-worker memory growth

### If tasks are failing:
1. **Check individual task errors** in Dask dashboard
2. **Reduce parallelism**: Lower max_concurrent_chunks
3. **Check file system limits**: Max open files, etc.

## Recommended Progression

Start with conservative settings and increase gradually:

1. **Start**: Use `config_subset_aux_goamazon_chunked.yml` (conservative)
2. **If stable**: Switch to `config_subset_aux_goamazon_aggressive.yml`
3. **Monitor**: Watch memory usage and adjust if needed
4. **Fine-tune**: Adjust chunk_size and max_concurrent_chunks based on performance

## NUMA Optimizations

For AMD EPYC with 4 NUMA domains, the SLURM script includes:
```bash
export OMP_PROC_BIND=spread
export OMP_PLACES=cores
export GOMP_CPU_AFFINITY="0-127"
```

## Expected Performance

Based on your current progress (half of June in 4 hours):
- **Current rate**: ~15 days in 4 hours = ~3.75 days/hour
- **Conservative config**: ~6-7 days/hour (2x speedup)
- **Aggressive config**: ~12-15 days/hour (4x speedup)
- **Full year**: 214 days remaining ÷ 12 days/hour = ~18 hours

## File System Considerations

If using networked storage (Lustre, GPFS, etc.):
- Consider striping settings for output directory
- Monitor network bandwidth usage
- May need to reduce parallelism if I/O bound

## Memory Estimation Formula

```
Memory per chunk = chunk_size × 96 timesteps × ~10MB per output file
Total memory = max_concurrent_chunks × memory_per_chunk
```

Example for aggressive config:
```
Memory per chunk = 21 × 96 × 10MB = ~20GB
Total memory = 12 × 20GB = ~240GB
```
