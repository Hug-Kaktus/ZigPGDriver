const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;

pub fn parse(
        self: *Connection,
        query_name: []const u8,
        query: []const u8,
        param_types: std.ArrayList(i32),
    ) !void {
    const message_len: usize = 4 +
                             query_name.len + 1 +
                             query.len + 1 +
                             2 +
                             param_types.items.len*4;
    try self.writer.interface.writeByte('P');
    try self.writer.interface.writeInt(i32, @intCast(message_len), .big);
    try self.writer.interface.writeAll(query_name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(query);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeInt(i16, @intCast(param_types.items.len), .big);
    for (param_types.items) |param_type| {
        std.debug.print("param_type: {d}", .{param_type});
        try self.writer.interface.writeInt(i32, param_type, .big);
    }
    try self.writer.interface.flush();
    try sync(self);
    var reader = self.reader.interface();
    const msg_type: u8 = try reader.takeByte();
    std.debug.print("msg_type: {c}\n", .{msg_type});
    _ = try reader.takeInt(i32, .big);
    switch (msg_type) {
        '1' => {
            std.debug.print("Parse complete\n", .{});
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

pub fn sync(self: *Connection) !void {
    try self.writer.interface.writeByte('S');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
}

const ParameterValue = struct {
    length: i32,
    value: []const u8,
};

pub fn bind(
    self: *Connection,
    destination_portal: []const u8,
    source_prepared_statement: []const u8,
    parameter_format_codes: std.ArrayList(i16),
    parameter_values: std.ArrayList(ParameterValue),
    result_column_format_codes: std.ArrayList(i16),
) !void {
    try self.writer.interface.writeByte('B');
    try self.writer.interface.writeInt(i32, 235423, .big);
    try self.writer.interface.write(destination_portal);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.write(source_prepared_statement);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeInt(i16, @intCast(parameter_format_codes.items.len), .big);
    for (parameter_format_codes) |code| {
        try self.writer.interface.writeInt(i16, code, .big);
    }
    try self.writer.interface.writeInt(i16, @intCast(parameter_values.items.len), .big);
    for (parameter_values) |value| {
        try self.writer.interface.writeInt(i16, @intCast(value.length), .big);
        try self.writer.interface.write(value.value);
    }
    self.writer.interface.writeInt(i16, @intCast(result_column_format_codes.items.len), .big);
    for (result_column_format_codes) |code| {
        try self.writer.interface.writeInt(i16, code, .big);
    }
    try self.writer.interface.flush();
}

pub fn execute(self: *Connection, portal_name: []const u8, max_rows_number: i32) !void {
    try self.writer.interface.writeByte('E');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(portal_name.len)) + 4, .big);
    try self.writer.interface.writeAll(portal_name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeInt(i32, max_rows_number, .big);
    try self.writer.interface.flush();
}

pub fn flush(self: *Connection) !void {
    try self.writer.interface.writeByte('F');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
}

pub fn close(self: *Connection, object_type: u8, name: []const u8,) !void {
    try self.writer.interface.writeByte(object_type);
    try self.writer.interface.writeInt(i32, 5 + name.len + 1, .big);
    try self.writer.interface.write(name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
    const msg_type = try self.reader.interface().takeByte();
    _ = try self.reader.interface().takeInt(i32, .big);
    switch (msg_type) {
        '3' => {
            std.debug.print("Close complete\n", .{});
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

pub fn describe(self: *Connection, object_type: u8, name: []const u8) !void {
    try self.writer.interface.writeByte('D');
    try self.writer.interface.writeInt(i32, 4 + 1 + name.len + 1, .big);
    try self.writer.interface.writeByte(object_type);
    try self.writer.interface.write(name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
    while (true) {
        const msg_type = try self.reader.interface().takeByte();
        _ = try self.reader.interface().takeInt(i32, .big);
        switch (msg_type) {
            'T' => {
                std.debug.print("Row Description", .{});
            },
            't' => {
                std.debug.print("Parameter Description", .{});
            },
            'n' => {
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
        }
    }
}

pub fn cancel(self: *Connection, process_id: i32, secret_key: []const u8) !void {
    self.writer.interface.writeInt(i32, 8 + @as(i32, @intCast(secret_key.len)), .big);
    self.writer.interface.writeInt(i32, 80877102, .big);
    self.writer.interface.writeInt(i32, process_id, .big);
    self.writer.interface.writeAll(secret_key);
    self.writer.interface.flush();
}
