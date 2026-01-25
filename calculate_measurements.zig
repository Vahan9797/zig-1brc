const std = @import("std");

const MinMaxMean = struct {
    min  : f32,
    max  : f32,
    sum  : f32,
    count: f32
};

const  input_file_path = "./measurements.txt";
const output_file_path = "./results.txt";

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

    const page_size = std.heap.pageSize() << 4;
    var stdin_buffer: [page_size]u8 = undefined;
    const cwd = std.fs.cwd();

    var input_file = try cwd.openFile(input_file_path, .{ .lock = .shared });
    defer input_file.close();

    var file_reader = input_file.reader(&stdin_buffer);
    var reader = &file_reader.interface;

    while (reader.takeDelimiterExclusive('\n')) |line| {
        var tokens = std.mem.tokenizeScalar(u8, line, ';');

        const key = try allocator.dupe(u8, tokens.next().?);
        const value = try std.fmt.parseFloat(f32, tokens.next().?);

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

    var output_file = try cwd.createFile(output_file_path, .{ .truncate = true });
    var stdout_buffer: [page_size]u8 = undefined;

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
