const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const parseKeyValuePayload = helpers.parseKeyValuePayload;
const authenticate = @import("authentication.zig").authenticate;

pub fn startup(
    self: *Connection,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    replication: []const u8,
) !void {
    const startup_msg_len: usize = 4 + 4 + 1 + user.len + 1 + 8 + 1 +
        database.len + 1 + 11 + 1 + replication.len + 1 + 1 + 4;
    try self.writer.interface.writeInt(i32, @intCast(startup_msg_len), .big);
    try self.writer.interface.writeInt(i32, 196610, .big);
    try self.writer.interface.writeAll("user");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(user);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll("database");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(database);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll("replication");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(replication);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    while (true) {
        const msg_type = try self.reader.interface().takeByte();
        const payload_len: usize = @intCast(try self.reader.interface().takeInt(i32, .big) - 4);

        switch (msg_type) {
            'K' => {
                self.process_id = try self.reader.interface().takeInt(i32, .big);
                self.secret_key_len = @as(i32, @intCast(payload_len)) - 4;
                _ = try self.reader.interface().readSliceShort(self.secret_key[0 .. payload_len - 4]);
            },
            'R' => {
                try authenticate(self, @intCast(payload_len), password);
            },
            'Z' => {
                // transaction_status
                _ = try self.reader.interface().takeByte();
                return;
            },
            'S' => {
                try parseKeyValuePayload(self);
            },
            'E' => {
                const error_message = try buildMessage(self.allocator, self.reader.interface());
                std.debug.print("{s}\n", .{error_message.items});
                return error.ServerError;
            },
            'N' => {
                const notice_message = try buildMessage(self.allocator, self.reader.interface());
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => std.debug.print("Unknown msg: {c}\n", .{msg_type}),
        }
    }
}
