const std = @import("std");
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const Connection = @import("connection.zig").Connection;

const copy = @import("copy.zig");
const copyIn = copy.copyIn;
const copyFail = copy.copyFail;

const types = @import("types.zig");
const PgType = types.PgType;
const oidToType = types.oidToType;
const Value = types.Value;
const convertValue = types.convertValue;
const FieldData = types.FieldData;
const Row = types.Row;
const QueryResult = types.QueryResult;


fn decodeValueText(allocator: std.mem.Allocator, T: PgType, bytes: ?[]const u8) !Value {
    if (bytes == null) return Value.Null;
    const b = bytes.?;

    return switch (T) {
        .Int4 => Value{ .Int4 = try std.fmt.parseInt(i32, b, 10)},
        .Int8 => Value{ .Int8 = try std.fmt.parseInt(i64, b, 10)},
        .Float4 => Value{ .Float4 = try std.fmt.parseFloat(f32, b)},
        .Float8 => Value{ .Float8 = try std.fmt.parseFloat(f64, b)},
        .Bool => Value{ .Bool = (b.len > 0 and b[0] == 't')},
        .Text => Value{ .Text = b },
        .Unknown => Value{ .Text = try allocator.dupe(u8, b)},
    };
}

fn decodeValueBinary(typ: PgType, bytes: []const u8) !Value {
    return switch (typ) {
        .Int4 => Value{
            .Int4 = std.mem.readInt(i32, bytes[0..4], .big),
        },
        .Int8 => Value{
            .Int8 = std.mem.readInt(i64, bytes[0..8], .big),
        },
        .Float4 => Value{
            .Float4 = @bitCast(std.mem.readInt(u32, bytes[0..4], .big)),
        },
        .Float8 => Value{
            .Float8 = @bitCast(std.mem.readInt(u64, bytes[0..8], .big)),
        },
        .Bool => Value{
            .Bool = bytes[0] != 0,
        },
        .Text => Value{ .Text = bytes },
        else => Value{ .Text = bytes },
    };
}

pub fn decodeRowToStruct(
    comptime T: type,
    row: Row,
    fields: []FieldData,
) !T {
    var result: T = undefined;

    inline for (@typeInfo(T).@"struct".fields) |field| {
        var found = false;

        for (fields, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, field.name)) {
                @field(result, field.name) = try convertValue(field.type, row.values[i]);
                found = true;
                break;
            }
        }

        if (!found) {
            if (@typeInfo(field.type) == .optional) {
                @field(result, field.name) = null;
            } else {
                return error.MissingField;
            }
        }
    }

    return result;
}

pub fn parseFieldData(self: *Connection) !std.ArrayList(FieldData) {
    var reader = self.reader.interface();
    const field_number = try reader.takeInt(i16, .big);
    var array = try std.ArrayList(FieldData).initCapacity(self.allocator, @intCast(field_number));
    var k: u16 = 0;
    while (k < field_number) {
        const name = try self.allocator.dupe(u8, (try reader.takeDelimiter(0)).?);
        const table_object_id = try reader.takeInt(i32, .big);
        const column_attribute_number = try reader.takeInt(i16, .big);
        const data_type_object_id = try reader.takeInt(i32, .big);
        const data_type_size = try reader.takeInt(i16, .big);
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
    return array;
}

pub fn parseRowData(self: *Connection, fields: []const FieldData) !Row {
    var reader = self.reader.interface();
    const values_number = try reader.takeInt(i16, .big);
    var values = try self.allocator.alloc(Value, @intCast(values_number));

    var k: usize = 0;
    while (k < values_number) : (k += 1) {
        const value_len: i32 = try reader.takeInt(i32, .big);

        var raw: ?[]const u8 = null;

        if (value_len == -1) {
            raw = null;
        } else {
            // might be error prone due to buf being const and not var
            const buf = try self.allocator.alloc(u8, @intCast(value_len));
            _ = try reader.readSliceShort(buf);
            raw = buf;
        }

        const T = oidToType(fields[k].data_type_object_id);
        const format = fields[k].format_code;
        values[k] = try decodeValueText(self.allocator, T, raw);
        if (raw == null) {
            values[k] = Value.Null;
        } else if (format == 0) {
            values[k] = try decodeValueText(self.allocator, T, raw);
        } else {
            values[k] = try decodeValueBinary(T, raw.?);
        }
    }

    return Row{ .values = values };
}

pub fn queryWithFields(self: *Connection, sql: []const u8) !QueryResult {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    const a = arena.allocator();
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(sql.len)) + 1, .big);
    try self.writer.interface.writeAll(sql);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    var fields: ?std.ArrayList(FieldData) = null;
    var rows = try std.ArrayList(Row).initCapacity(a, 16);
    while (true) {
        const msg_type = try self.reader.interface().takeByte();
        _ = try self.reader.interface().takeInt(i32, .big);
        std.debug.print("msg_type: {c}\n", .{msg_type});
        switch (msg_type) {
            'T' => {
                fields = try parseFieldData(self);
            },
            'D' => {
                const row = try parseRowData(self, fields.?.items);
                try rows.append(a, row);
            },
            'C' => {
                const command_tag: []const u8 = (try self.reader.interface().takeDelimiter(0)).?;
                std.debug.print("{s}\n", .{command_tag});
                return QueryResult{
                    .arena = arena,
                    .fields = if (fields) |f| f else try std.ArrayList(FieldData).initCapacity(a, 0),
                    .rows = rows,
                };
            },
            'I' => {
                return QueryResult{
                    .arena = arena,
                    .fields = if (fields) |f| f else try std.ArrayList(FieldData).initCapacity(a, 0),
                    .rows = rows,
                };
            },
            'Z' => {
                _ = try self.reader.interface().takeByte();
                return QueryResult{
                    .arena = arena,
                    .fields = if (fields) |f| f else try std.ArrayList(FieldData).initCapacity(a, 0),
                    .rows = rows,
                };
            },
            'E' => {
                const error_message = try buildMessage(a, self.reader.interface());
                std.debug.print("{s}\n", .{error_message.items});
                return error.ServerError;
            },
            'N' => {
                const notice_message = try buildMessage(a, self.reader.interface());
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unsupported message type {c}\n", .{msg_type});
            },
        }
    }
}

pub fn queryTyped(
    self: *Connection,
    comptime T: type,
    sql: []const u8,
) !std.ArrayList(T) {
    const result = try queryWithFields(self, sql);
    defer result.deinit();

    var out = try std.ArrayList(T).initCapacity(self.allocator, 128);

    for (result.rows.items) |row| {
        const obj = try decodeRowToStruct(
            T,
            row,
            result.fields.items,
        );
        try out.append(self.allocator, obj);
    }

    return out;
}
