const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const parseKeyValuePayload = helpers.parseKeyValuePayload;

pub fn copyFromReader(self: *Connection, table_name: []const u8, reader: anytype) !void {
    try self.writer.interface.writeByte('Q');
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "COPY ");
    try sql.appendSlice(self.allocator, table_name);
    try sql.appendSlice(self.allocator, " FROM STDIN WITH (FORMAT csv)");
    try self.writer.interface.writeInt(i32, @intCast(4 + sql.items.len + 1), .big);
    try self.writer.interface.writeAll(sql.items);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    var r = &self.reader;

    while (true) {
        const msg_type = try r.interface.takeByte();
        _ = try r.interface.takeInt(i32, .big);

        switch (msg_type) {
            'G' => {
                _ = try r.interface.takeInt(i8, .big);
                const cols = try r.interface.takeInt(i16, .big);

                for (0..@intCast(cols)) |_| {
                    _ = try r.interface.takeInt(i16, .big);
                }
                break;
            },
            'S' => try parseKeyValuePayload(self),
            'E' => {
                var error_message = try buildMessage(self.allocator, r);
                defer error_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{error_message.items});
                return error.ServerError;
            },
            'N' => {
                var notice_message = try buildMessage(self.allocator, r);
                defer notice_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unsupported message type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }

    var buffer: [4096]u8 = undefined;
    while (true) {
        // this line tells the reader to fill in the buffer with the data to be copied to the server
        const n = try reader.interface.readSliceShort(&buffer);
        if (n == 0) break;

        try self.writer.interface.writeByte('d');
        try self.writer.interface.writeInt(i32, @intCast(4 + n), .big);
        try self.writer.interface.writeAll(buffer[0..n]);
    }
    try copyDone(self);

    while (true) {
        const msg_type = try r.interface.takeByte();
        const len = try r.interface.takeInt(i32, .big);

        switch (msg_type) {
            'C' => _ = try r.interface.take(@intCast(len - 4)),
            'Z' => return,
            'E' => {
                var error_message = try buildMessage(self.allocator, r);
                defer error_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{error_message.items});
                return error.ServerError;
            },
            'N' => {
                var notice_message = try buildMessage(self.allocator, r);
                defer notice_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unsupported message type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn copyToWriter(self: *Connection, table_name: []const u8, writer: anytype) !void {
    try self.writer.interface.writeByte('Q');
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "COPY ");
    try sql.appendSlice(self.allocator, table_name);
    try sql.appendSlice(self.allocator, " TO STDOUT WITH (FORMAT csv)");
    try self.writer.interface.writeInt(i32, @intCast(4 + sql.items.len + 1), .big);
    try self.writer.interface.writeAll(sql.items);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    var r = &self.reader;

    while (true) {
        const msg_type = try r.interface.takeByte();
        const len = try r.interface.takeInt(i32, .big);
        const payload_len = len - 4;

        switch (msg_type) {
            'H' => {
                _ = try r.interface.takeInt(i8, .big);
                const cols = try r.interface.takeInt(i16, .big);

                for (0..@intCast(cols)) |_| {
                    _ = try r.interface.takeInt(i16, .big);
                }
            },
            'd' => {
                const data = try r.interface.take(@intCast(payload_len));
                try writer.interface.writeAll(data);
                try writer.interface.flush();
            },
            'c' => {},
            'C' => _ = try r.interface.take(@intCast(payload_len)),
            'Z' => return,
            'E' => {
                var error_message = try buildMessage(self.allocator, r);
                defer error_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{error_message.items});
                return error.ServerError;
            },
            'N' => {
                var notice_message = try buildMessage(self.allocator, r);
                defer notice_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unsupported message type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn copyDone(self: *Connection) !void {
    try self.writer.interface.writeByte('c');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
}

pub fn copyFail(self: *Connection, error_message: []const u8) !void {
    try self.writer.interface.writeByte('f');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(error_message.len)) + 1, .big);
    try self.writer.interface.writeAll(error_message);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}
