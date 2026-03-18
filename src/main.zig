const std = @import("std");
const Connection = @import("connection.zig").Connection;
const query = @import("query.zig").query;
const parse = @import("extended_query.zig").parse;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var conn = try Connection.connect(
        allocator,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "postgres",
        "false",
    );
    defer conn.close();

    var params = try std.ArrayList(i32).initCapacity(allocator, 0);
    defer params.deinit(allocator);
    try parse(&conn, "test_query", "SELECT 1;", params);
}
