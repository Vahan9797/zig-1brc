const std = @import("std");

const MinMaxMean = struct {
    min  : f32,
    max  : f32,
    sum  : f32,
    count: f32
};

const  input_file_path = "./measurements.txt";
const output_file_path = "./results_no_mmap.txt";

// currently there are only 413 unique entries
// but the original requirements state that they can be up to 10000
const hmap_capacity: usize = 10000;

const WYH_SEED: u64 = 0x2C6A9586D731E7C2;

const HashCtx = struct {
    pub fn hash(self: @This(), s: []const u8) u64 {
        _ = self;
        return std.hash.Wyhash.hash(WYH_SEED, s);
    }
    pub fn eql(self: @This(), a: []const u8, b: []const u8) bool {
        _ = self;
        return eql_impl(a, b);
    }
};

pub fn eql_impl(a: []const u8, b: []const u8) bool {
    if (a.len != b.len){
        @branchHint(.unlikely);
        return false;
    }

    if (a.len <= 16) {
        @branchHint(.likely);
        if (a.len < 4) {
            @branchHint(.unlikely);
            const x = (a[0] ^ b[0]) | (a[a.len - 1] ^ b[a.len - 1]) | (a[a.len / 2] ^ b[a.len / 2]);
            return x == 0;
        }
        var x: u32 = 0;
        for ([_]usize{ 0, a.len - 4, (a.len >> 3) * 4, a.len - 4 - ((a.len >> 3) * 4) }) |n| {
            x |= @as(u32, @bitCast(a[n..][0..4].*)) ^ @as(u32, @bitCast(b[n..][0..4].*));
        }
        return x == 0;
    }
    const Scan = if (std.simd.suggestVectorLength(u8)) |vec_size|
        struct {
            pub const size = vec_size;
            pub const Chunk = @Vector(size, u8);
            pub inline fn isNotEqual(chunk_a: Chunk, chunk_b: Chunk) bool {
                return @reduce(.Or, chunk_a != chunk_b);
            }
        };

    inline for (1..6) |s| {
        const n = 16 << s;
        if (n <= Scan.size and a.len <= n) {
            const V = @Vector(n >> 1, u8);
            var x = @as(V, a[0 .. n >> 1].*) ^ @as(V, b[0 .. n >> 1].*);
            x |= @as(V, a[a.len - n >> 1 ..][0 .. n >> 1].*) ^ @as(V, b[a.len - n >> 1 ..][0 .. n >> 1].*);
            const zero: V = @splat(0);
            return !@reduce(.Or, x != zero);
        }
    }

    for (0..(a.len - 1) / Scan.size) |i| {
        const a_chunk: Scan.Chunk = @bitCast(a[i * Scan.size ..][0..Scan.size].*);
        const b_chunk: Scan.Chunk = @bitCast(b[i * Scan.size ..][0..Scan.size].*);
        if (Scan.isNotEqual(a_chunk, b_chunk)) return false;
    }

    const last_a_chunk: Scan.Chunk = @bitCast(a[a.len - Scan.size ..][0..Scan.size].*);
    const last_b_chunk: Scan.Chunk = @bitCast(b[a.len - Scan.size ..][0..Scan.size].*);
    return !Scan.isNotEqual(last_a_chunk, last_b_chunk);
}

const IndiceVec = union(enum) {
    long: @Vector(3, usize),
    short: @Vector(2, usize)
};

inline fn fastParseChunks(comptime N: u8, comptime indiceVec: IndiceVec, str: []const u8) f32 {
    var raw_bytes: @Vector(N, u8) = undefined;
    const ascii_offset: @Vector(N, u8) = @splat('0');

    const indices = switch (indiceVec) {
        .long => |v| v,
        .short => |v| v
    };

    inline for (0..N) |idx|
        raw_bytes[idx] = str[indices[idx]];

    const digits_u8 = raw_bytes - ascii_offset;
    const digits_f32: @Vector(N, f32) = @floatFromInt(digits_u8);

    const weights = if (N == 3) @Vector(N, f32){ 10.0, 1.0, 0.1 } else @Vector(N, f32){ 1.0, 0.1 };
    const multiply = digits_f32 * weights;

    return @reduce(.Add, multiply);
}

// We only have 3 different cases for str.len: 3, 4 and 5
inline fn fastParseFloat(str: []const u8) f32 {
    const negative = str[0] == '-';

    if (str.len == 5) { // 5 length can only have a negative number
        @branchHint(.unlikely);

        return -1 * fastParseChunks(3, IndiceVec{ .long = .{ 1, 2, 4 } }, str);
    } else if (str.len == 3) { // 3 length can only have a positive number 0<n<10
        return fastParseChunks(2, IndiceVec{ .short = .{ 0, 2 } }, str);
    } else {
        @branchHint(.likely);

        if (negative) {
            @branchHint(.unlikely);

            return -1 * fastParseChunks(2, IndiceVec{ .short = .{ 1, 3 } }, str);
        } else {
            @branchHint(.likely);

            return fastParseChunks(3, IndiceVec{ .long = .{ 0, 1, 3 } }, str);
        }
    }
}

pub fn main() !void {
    std.debug.print("Starting measurements calculation\n", .{});
    var timer = try std.time.Timer.start();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var hmap = std.HashMapUnmanaged([]const u8, MinMaxMean, HashCtx, 80).empty;
    defer hmap.deinit(allocator);

    try hmap.ensureUnusedCapacity(allocator, hmap_capacity);

    const cache_line = 64 * 1024; // 64KB
    var stdin_buffer: [cache_line]u8 = undefined;
    const cwd = std.fs.cwd();

    var input_file = try cwd.openFile(input_file_path, .{ .lock = .shared });
    defer input_file.close();

    var file_reader = input_file.reader(&stdin_buffer);
    var reader = &file_reader.interface;

    while (reader.takeDelimiterExclusive('\n')) |line| {
        const tokenPos = std.mem.indexOfScalarPos(u8, line, 2, ';').?;

        const key = try allocator.dupe(u8, line[0..tokenPos]);
        const value = fastParseFloat(line[tokenPos+1..]);

        if (!hmap.contains(key)) {
            @branchHint(.unlikely);

            _= try hmap.fetchPut(allocator, key, .{
                .min   = value,
                .max   = value,
                .sum   = value,
                .count = 1
            });

            reader.toss(1);
            continue;
        }

        const stationValPtr: ?*MinMaxMean = hmap.getPtr(key);

        stationValPtr.?.*.min    = @min(stationValPtr.?.*.min, value);
        stationValPtr.?.*.max    = @max(stationValPtr.?.*.max, value);
        stationValPtr.?.*.sum   += value;
        stationValPtr.?.*.count += 1;

        reader.toss(1);
    } else |err| {
        switch (err) {
            error.EndOfStream => std.debug.print("Finished reading the input file\n", .{}),
            else => std.debug.print("Something went wrong while reading the file: {}\n", .{ err })
        }
    }

    // sort the map alphabetically
    var hmap_keys = try std.ArrayList([]const u8).initCapacity(allocator, hmap.count());
    defer hmap_keys.deinit(allocator);

    var iterator = hmap.iterator();

    while (iterator.next()) |entry| {
        try hmap_keys.append(allocator, entry.key_ptr.*);
    }

    std.sort.heap([]const u8, hmap_keys.items, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.less);

    var output_file = try cwd.createFile(output_file_path, .{ .truncate = true });
    var stdout_buffer: [cache_line]u8 = undefined;

    var file_writer = output_file.writer(&stdout_buffer);
    var writer = &file_writer.interface;

    for (hmap_keys.items) |key| {
        const value = hmap.get(key).?;
        const str = try std.fmt.allocPrint(
            allocator,
            "{s}={d:.1}/{d:.1}/{d:.1}\n",
            .{
                key,
                value.min,
                value.sum / value.count,
                value.max
            }
        );

        try writer.writeAll(str);
    }

    try writer.flush();

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_ms;

    std.debug.print("Done in {d} milliseconds.", .{ elapsed_ms });
}
