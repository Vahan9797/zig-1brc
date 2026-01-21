# Billion Row Challenge with Zig

A little late implementation of 1brc challenge, which was arranged by [Gunnar Morling](https://github.com/gunnarmorling/1brc)

Current stats (based on multiple tests using i7-11700 CPU):

- `create_measurements.zig`: `~40-60 seconds`
- `calculate_measurements.zig`: `TBD`

To run this you need to have the latest Zig compiler (`v0.15.2` at the moment of creating this):

```
zig build-exe -O ReleaseFast [file_name].zig && ./[file_name]
# or run directly
zig run -O ReleaseFast [file_name].zig
```