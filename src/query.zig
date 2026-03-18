const std = @import("std");
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const Connection = @import("connection.zig").Connection;


pub const FieldData = struct {
    table_object_id: i32,
    data_type_object_id: i32,
    type_modifier: i32,
    name: []const u8,
    data_type_size: i16,
    format_code: i16,
    column_attribute_number: i16,
};

pub fn parseFieldData(self: *Connection) !std.ArrayList(FieldData) {
    var reader = self.reader.interface();
    const field_number = reader.takeInt(i16, .big);
    var array = try std.ArrayList(FieldData).initCapacity(self.allocator, @intCast(field_number));
    var k: u16 = 0;
    while (k < field_number) {
        const name = (try reader.takeDelimiter(0)).?;
        const table_object_id = try reader.takeInt(i32, .big);
        const column_attribute_number = try reader.takeInt(i16, .big);
        const data_type_object_id = try reader.takeInt(i32, .big);
        const data_type_size = try reader.takeInt(i16);
        const type_modifier = try reader.takeInt(i32, .big);
        const format_code = try reader.takeInt(i16, .big);
        try array.append(self.allocator, FieldData{
            .table_object_id = table_object_id,
            .data_type_object_id = data_type_object_id,
            .type_modifier = type_modifier,
            .name = name,
            .data_type_size = data_type_size,
            .format_code = format_code,
            .column_attribute_number = column_attribute_number,
        });
        k += 1;
    }
    std.debug.print("FIELD DATA: {any}\n", .{array.getLast()});
    return array;
}

pub fn parseRowData(self: *Connection) !void {
    var reader = self.reader.interface();
    const values_number = try reader.takeInt(i16, .big);
    var k: u16 = 0;
    while (k < values_number) {
        const value_len: i32 = try reader.takeInt(i32, .big);
        if (value_len == -1) {
            const value = null;
            std.debug.print("value: {any}\n", .{value});
        } else {
            var value = std.ArrayList(u8).initCapacity(self.allocator, @intCast(value_len)); 
            _ = try reader.readSliceShort(&value.items);
            std.debug.print("value: {any}\n", .{value.items});
        }
        k += 1;
    }
}

pub fn query(self: *Connection, sql: []const u8) !void {
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(sql.len)) + 1, .big);
    try self.writer.interface.writeAll(sql);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    while (true) {
        var header: [5]u8 = undefined;
        _ = try self.reader.interface().readSliceShort(&header);
        const msg_type = header[0];
        const response_len = std.mem.readInt(u32, header[1..5], .big) - 4;
        switch (msg_type) {
            'T' => {
                var response_msg: [128]u8 = undefined;
                _ = try self.reader.interface().readSliceShort(response_msg[0..response_len]);
                _ = try parseFieldData(response_msg[0..response_len]);
            },
            'D' => {
                var response_msg: [128]u8 = undefined;
                _ = try self.reader.interface().readSliceShort(response_msg[0..response_len]);
                try parseRowData(response_msg[0..response_len]);
            },
            'C' => {
                const command_tag: []const u8 = (try self.reader.interface().takeDelimiter(0)).?;
                std.debug.print("{s}\n", .{command_tag});
                return;
            },
            'I' => {
                return;
            },
            'Z' => {
                _ = try self.reader.interface().takeByte();
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
            else => {
                std.debug.print("Unsupported message type {c}\n", .{msg_type});
            },
        }
    }
}
