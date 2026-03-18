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
        const startup_msg_len: usize = 4 + 4 + 1 + user.len + 1 + 8 + 1 + database.len + 1 + 11 + 1 + replication.len + 1 + 1 + 4;
        try self.writer.interface.writeInt(i32, @intCast(startup_msg_len), .big);
        try self.writer.interface.flush();

        while (true) {
            const msg_type = try self.reader.interface().takeByte();
            const payload_len = try self.reader.interface().takeInt(i32, .big) - 4;

            switch (msg_type) {
                'K' => {
                    const process_id: i32 = try self.reader.interface().takeInt(i32, .big);
                    var secret_key: [256]u8 = undefined;
                    _ = try self.reader.interface().readSliceShort(secret_key[0 .. payload_len - 4]);
                    std.debug.print("process_id: {d}, secret_key: {x}\n", .{ process_id, secret_key[0 .. payload_len - 4] });
                },
                'R' => {
                    try authenticate(self, payload_len, password);
                },
                'S' => {
                    var payload_buf: [128]u8 = undefined;
                    _ = try self.reader.interface().readSliceShort(payload_buf[0..payload_len]);
                    const parameters = try parseKeyValuePayload(&payload_buf);
                    var it = parameters.iterator();
                    while (it.next()) |e| {
                        _ = e.key_ptr.*;
                        _ = e.value_ptr.*;
                        // std.debug.print("{s}: {s}\n", .{ k, v });
                    }
                },
                'Z' => {
                    const transaction_status: u8 = try self.reader.interface().takeByte();
                    std.debug.print("transaction_status: {c}\n", .{transaction_status});
                    return;
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
