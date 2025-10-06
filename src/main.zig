const std = @import("std");
const pg = @import("root.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var conn = try pg.Connection.connect(
        allocator,
        "127.0.0.1",
        5432,
        "postgres",
        "yourpassword",
        "postgres",
    );
    defer conn.close();

    try conn.query("SELECT 1;");
}
