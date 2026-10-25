const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Row = @import("types.zig").Row;

pub const ServerCursor = struct {
    connection: Connection,
    name: []const u8,
    binary: bool,
    scrollable: bool,
    withhold: bool,

    pub fn init(
        connection: Connection,
        name: []const u8,
        binary: bool,
        scrollable: ?bool,
        withhold: bool,
    ) !ServerCursor {
        return .{
            .connection = connection,
            .name = name,
            .binary = binary,
            .scrollable = scrollable,
            .withhold = withhold,
        };
    }

    pub fn fetchbatch(self: *ServerCursor) !std.ArrayList(Row) {}

    pub fn fetchall(self: *ServerCursor) !std.ArrayList(Row) {}
};
