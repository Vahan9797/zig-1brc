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

inline fn fastParseFloat(str: []u8) f32 {
    const dotPos = str.len - 2; // we know the precision after dot is a single digit
    const exp: u8 = str[str.len - 1] - '0';

    const negative = str[0] == '-';
    const base = if (negative) str[1..dotPos] else str[0..dotPos];

    // we also know that temp cannot be <-100 or >100
    const res_int: u16 = if (base.len == 2) 10 * (base[0] - '0') + (base[1] - '0') else base[0] - '0';

    // Some numbers are getting IEEE754 trailing incorrections (which is expected)
    // e.g. 14.9 = 14.900001
    // So performing the calculation as f64
    // then float casting back to f32 gets rid of trailing exponent bits
    // and does not affect performance
    @setFloatMode(.optimized);
    const res_float: f32 = @as(f32, @floatCast(@as(f64, @floatFromInt(res_int * 10 + exp)) / 10.0));

    return if (negative) -res_float else res_float;
}

pub fn main() !void {
    std.debug.print("Starting measurements calculation\n", .{});
    var timer = try std.time.Timer.start();

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();

    const gpa_allocator = gpa.allocator();

    var arena = std.heap.ArenaAllocator.init(gpa_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var hmap = std.StringArrayHashMap(MinMaxMean).init(allocator);
    defer hmap.deinit();

    try hmap.ensureUnusedCapacity(hmap_capacity);

    const cache_line = 64 * 1024; // 64KB
    var stdin_buffer: [cache_line]u8 = undefined;
    const cwd = std.fs.cwd();

    var input_file = try cwd.openFile(input_file_path, .{ .lock = .shared });
    defer input_file.close();

    var file_reader = input_file.reader(&stdin_buffer);
    var reader = &file_reader.interface;

    while (reader.takeDelimiterExclusive('\n')) |line| {
        const tokenPos = std.mem.indexOfPosLinear(u8, line, 2, ";").?;

        const key = try allocator.dupe(u8, line[0..tokenPos]);
        const value = fastParseFloat(line[tokenPos+1..]);

        if (!hmap.contains(key)) {
            @branchHint(.unlikely);

            _= try hmap.fetchPut(key, .{
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
    hmap.sort(struct {
        map_ptr: *std.StringArrayHashMap(MinMaxMean),

        pub fn lessThan(self: @This(), a_idx: usize, b_idx: usize) bool {
            const keys = self.map_ptr.keys();

            return std.mem.order(u8, keys[a_idx], keys[b_idx]) == .lt;
        }
    }{ .map_ptr = &hmap });

    var output_file = try cwd.createFile(output_file_path, .{ .truncate = true });
    var stdout_buffer: [cache_line]u8 = undefined;

    var file_writer = output_file.writer(&stdout_buffer);
    var writer = &file_writer.interface;

    var iterator = hmap.iterator();

    while (iterator.next()) |entry| {
        const str = try std.fmt.allocPrint(
            allocator,
            "{s}={d:.1}/{d:.1}/{d:.1}\n",
            .{
                entry.key_ptr.*,
                entry.value_ptr.*.min,
                entry.value_ptr.*.sum / entry.value_ptr.*.count,
                entry.value_ptr.*.max
            }
        );

        try writer.writeAll(str);
    }

    try writer.flush();

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_ms;

    std.debug.print("Done in {d} milliseconds.", .{ elapsed_ms });
}
