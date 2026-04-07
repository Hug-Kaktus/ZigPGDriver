const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const parseKeyValuePayload = helpers.parseKeyValuePayload;

pub fn copyIn(self: *Connection, data: [][]const u8) !void {
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + 15 + 1, .big);
    try self.writer.interface.writeAll("COPY FROM STDIN");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
    const msg_type = try self.reader.interface().takeByte();
    _ = try self.reader.interface().takeInt(i32, .big);
    switch (msg_type) {
        'G' => {
            const copy_format: i8 = try self.reader.interface().takeInt(i8, .big);
            std.debug.print("copy_format: {d}\n", .{copy_format});
            const number_of_columns = try self.reader.interface().takeInt(i16, .big);
            var format_codes = try std.ArrayList(i16).initCapacity(self.allocator, @intCast(number_of_columns));
            defer format_codes.deinit(self.allocator);
            for (0..@intCast(number_of_columns)) |_| {
                try format_codes.append(self.allocator, try self.reader.interface().takeInt(i16, .big));
            }
            if (data.len > 0) {
                copyIn(self, data) catch {
                    try copyFail(self, "An unexpected error occured.");
                };
            } else {
                return error.NoCopyInData;
            }
            try copyDone(self);
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
        else => {
            std.debug.print("Unknown message type: {c}\n", .{msg_type});
        }
    }
    for (data) |row| {
        try self.writer.interface.writeByte('d');
        try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(row.len)), .big);
        _ = try self.writer.interface.write(row);
        try self.writer.interface.flush();
    }
}

pub fn copyOut(self: *Connection, buf: *std.ArrayList([]const u8)) !void {
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + 14 + 1, .big);
    try self.writer.interface.writeAll("COPY TO STDOUT");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
    while (true) {
        const msg_type = try self.reader.interface().takeByte();
        const payload_len = try self.reader.interface().takeInt(i32, .big) - 4;
        switch (msg_type) {
            'H' => {
                const copy_format: i8 = try self.reader.interface().takeInt(i8, .big);
                std.debug.print("copy_format: {d}\n", .{copy_format});
                const number_of_columns = try self.reader.interface().takeInt(i16, .big);
                const format_codes = try std.ArrayList(i16).initCapacity(self.allocator, @intCast(number_of_columns));
                defer format_codes.deinit(self.allocator);
                for (0..number_of_columns) |_| {
                    format_codes.append(self.reader.interface().takeInt(i16, .big));
                }

                try copyDone(self);
            },
            'd' => {
                buf.append(self.allocator, self.reader.interface().take(@intCast(payload_len)));
            },
            'c' => {
                std.debug.print("Copy out done.\n", .{});
                return;
            },
            'S' => {
                parseKeyValuePayload(self);
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
