# Billion Row Challenge with Zig

A little late implementation of 1brc challenge, which was arranged by [Gunnar Morling](https://github.com/gunnarmorling/1brc)

Current stats (based on multiple tests using i7-11700 CPU):

- `create_measurements.zig`: `~50 seconds`
- `calculate_single_thread_no_mmap.zig`: `~25 seconds`
- `calculate_single_thread_mmap.zig`: `~15 seconds`
- `calculate_multi_thread_mmap.zig`: `~2 seconds`

To run this you need to have the latest Zig compiler (`v0.15.2` at the moment of creating this):

```
zig build-exe -O ReleaseFast [file_name].zig && ./[file_name]
# or run directly
zig run -O ReleaseFast [file_name].zig
```
