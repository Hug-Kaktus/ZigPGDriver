const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const types = @import("types.zig");
const query_file = @import("query.zig");
const parseFieldData = query_file.parseFieldData;
const parseRowData = query_file.parseRowData;
const decodeRowToStruct = query_file.decodeRowToStruct;

const PgType = types.PgType;
const oidToType = types.oidToType;
const Value = types.Value;
const convertValue = types.convertValue;
const FieldData = types.FieldData;
const Row = types.Row;
const QueryResult = types.QueryResult;

const PreparedStatement = struct {
    name: []const u8,
    fields: std.ArrayList(FieldData),
    parameter_count: i32,
    parameters: std.ArrayList(i32),

    pub fn show(self: *const PreparedStatement) void {
        std.debug.print("Prepared statement \"{s}\"\n", .{self.name});
        std.debug.print("Fields:\n", .{});
        for (self.fields.items) |field| {
            field.show();
        }
        std.debug.print("Parameter count: {d}\n", .{self.parameter_count});
        std.debug.print("Parameters:\n", .{});
        for (self.parameters.items, 1..) |parameter, i| {
            std.debug.print("{d}. {s}\n", .{i, @tagName(oidToType(parameter))});
        }
    }
};

const BindedPreparedStatement = struct {
    prepared_statement: *const PreparedStatement,
    portal_name: []const u8,

    pub fn show(self: *const BindedPreparedStatement) void {
        self.prepared_statement.show();
        std.debug.print("Portal name: {s}\n", .{self.portal_name});
    }
};

const QueryState = enum {
    pending,
    done,
    failed,
    skipped,
};

const PendingQuery = struct {
    arena: std.heap.ArenaAllocator,
    prepared_statement: PreparedStatement,
    rows: std.ArrayList(Row),
    state: QueryState,
    error_message: ?[]u8,
};

pub fn parseMsg(
        self: *Connection,
        query_name: []const u8,
        query: []const u8,
        param_types: *std.ArrayList(i32),
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
        try self.writer.interface.writeInt(i32, param_type, .big);
    }
    try self.writer.interface.flush();
}

pub fn sync(self: *Connection) !void {
    try self.writer.interface.writeByte('S');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
}

pub const ParameterValue = struct {
    length: i32,
    value: []const u8,
};

pub fn bindMsg(
    self: *Connection,
    destination_portal: []const u8,
    source_prepared_statement: *const PreparedStatement,
    parameter_format_codes: std.ArrayList(i16),
    parameter_values: std.ArrayList(ParameterValue),
    result_column_format_codes: std.ArrayList(i16),
) !void {
    try self.writer.interface.writeByte('B');
    const len = 4 +
    destination_portal.len + 1 +
    source_prepared_statement.name.len + 1 +
    2 + parameter_format_codes.items.len * 2 +
    2 +
    blk: {
        var sum: usize = 0;
        for (parameter_values.items) |v| {
            if (v.length == -1) {
                sum += 4;
            } else {
                sum += 4 + @as(usize, @intCast(v.length));
            }
        }
        break :blk sum;
    } +
    2 + result_column_format_codes.items.len * 2;

    try self.writer.interface.writeInt(i32, @intCast(len), .big);
    try self.writer.interface.writeAll(destination_portal);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(source_prepared_statement.name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeInt(i16, @intCast(parameter_format_codes.items.len), .big);
    for (parameter_format_codes.items) |code| {
        try self.writer.interface.writeInt(i16, code, .big);
    }
    try self.writer.interface.writeInt(i16, @intCast(parameter_values.items.len), .big);
    for (parameter_values.items) |value| {
        try self.writer.interface.writeInt(i32, @intCast(value.length), .big);
        try self.writer.interface.writeAll(value.value);
    }
    try self.writer.interface.writeInt(i16, @intCast(result_column_format_codes.items.len), .big);
    for (result_column_format_codes.items) |code| {
        try self.writer.interface.writeInt(i16, code, .big);
    }
    try self.writer.interface.flush();
}

pub fn bindPreparedStatement(
    self: *Connection,
    destination_portal: []const u8,
    source_prepared_statement: *const PreparedStatement,
    parameter_format_codes: std.ArrayList(i16),
    parameter_values: std.ArrayList(ParameterValue),
    result_column_format_codes: std.ArrayList(i16),
)
!BindedPreparedStatement {
    try bindMsg(
        self,
        destination_portal,
        source_prepared_statement,
        parameter_format_codes,
        parameter_values,
        result_column_format_codes
    );
    try flush(self);
    var reader = self.reader.interface();
    while (true) {
        const msg_type = try reader.takeByte();
        _ = try reader.takeInt(i32, .big);
        switch (msg_type) {
            '2' => {
                return BindedPreparedStatement{
                    .prepared_statement = source_prepared_statement,
                    .portal_name = destination_portal,
                };
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

pub fn executeMsg(self: *Connection, portal_name: []const u8, max_rows_number: i32) !void {
    try self.writer.interface.writeByte('E');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(portal_name.len)) + 1 + 4, .big);
    try self.writer.interface.writeAll(portal_name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeInt(i32, max_rows_number, .big);
    try self.writer.interface.flush();
}

pub fn flush(self: *Connection) !void {
    try self.writer.interface.writeByte('H');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
}

pub fn close(self: *Connection, object_type: u8, name: []const u8,) !void {
    try self.writer.interface.writeByte('C');
    try self.writer.interface.writeInt(i32, 4 + 1 + name.len + 1, .big);
    try self.writer.interface.writeByte(object_type);
    try self.writer.interface.writeAll(name);
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
    try self.writer.interface.writeInt(i32, 4 + 1 + @as(i32, @intCast(name.len)) + 1, .big);
    try self.writer.interface.writeByte(object_type);
    try self.writer.interface.writeAll(name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn prepare(
    self: *Connection,
    name: []const u8,
    sql: []const u8,
    parameter_types: *std.ArrayList(i32),
) !PreparedStatement {
    var prepared_statement = PreparedStatement{
        .name = name,
        .fields = undefined,
        .parameter_count = undefined,
        .parameters = undefined,
    };

    try parseMsg(self, name, sql, parameter_types);
    try flush(self);
    parameter_types.deinit(self.allocator);

    var reader = self.reader.interface();
    while (true) {
        const msg_type = try reader.takeByte();
        _ = try reader.takeInt(i32, .big);
        switch (msg_type) {
            '1' => {
                try describe(self, 'S', name);
                try flush(self);
            },
            'T' => {
                prepared_statement.fields = try parseFieldData(self);
                return prepared_statement;
            },
            't' => {
                prepared_statement.parameters = try std.ArrayList(i32).initCapacity(self.allocator, 4);
                prepared_statement.parameter_count = try reader.takeInt(i16, .big);
                for (0..@intCast(prepared_statement.parameter_count)) |_| {
                    try prepared_statement.parameters.append(self.allocator, try reader.takeInt(i32, .big));
                }
            },
            'E' => {
                const err = try buildMessage(self.allocator, reader);
                std.debug.print("{s}\n", .{err.items});
                return error.ServerError;
            },
            'N' => {
                const notice_message = try buildMessage(self.allocator, self.reader.interface());
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unknown msg type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn executeQuery(
    self: *Connection,
    binded_prepared_statement: *const BindedPreparedStatement,
    max_rows_number: i32,
    ) !QueryResult {
    try executeMsg(self, binded_prepared_statement.portal_name, max_rows_number);
    try sync(self);
    var reader = self.reader.interface();

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    const a = arena.allocator();
    var rows = try std.ArrayList(Row).initCapacity(a, 8);

    while (true) {
        const msg_type = try reader.takeByte();
        _ = try reader.takeInt(i32, .big);
        switch (msg_type) {
            'D' => {
                const row = try parseRowData(self, binded_prepared_statement.prepared_statement.fields.items);
                try rows.append(self.allocator, row);
            },
            'C' => {
                _ = try reader.takeDelimiter(0);
            },
            'Z' => {
                _ = try reader.takeByte();
                return QueryResult{
                    .arena = arena,
                    .fields = binded_prepared_statement.prepared_statement.fields,
                    .rows = rows,
                };
            },
            'n' => {
                return QueryResult{
                    .arena = arena,
                    .fields = try std.ArrayList(FieldData).initCapacity(self.allocator, 0),
                    .rows = try std.ArrayList(Row).initCapacity(a, 0),
                };
            },
            'E' => {
                const err = try buildMessage(a, reader);
                std.debug.print("{s}\n", .{err.items});
                return error.ServerError;
            },
            'N' => {
                const notice_message = try buildMessage(self.allocator, self.reader.interface());
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unknown msg type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn executeQueryTyped(
    self: *Connection,
    comptime T: type,
    binded_prepared_statement: *const BindedPreparedStatement,
    max_rows_number: i32,
    ) !std.ArrayList(T) {
    var result = try executeQuery(self, binded_prepared_statement, max_rows_number);
    defer result.deinit();
    var out = try std.ArrayList(T).initCapacity(self.allocator, 8);
    for (result.rows.items) |row| {
        const obj = try decodeRowToStruct(T, row, result.fields.items);
        try out.append(self.allocator, obj);
    }
    return out;
}

pub fn cancel(self: *Connection, process_id: i32, secret_key: []const u8) !void {
    try self.writer.interface.writeInt(i32, 8 + @as(i32, @intCast(secret_key.len)), .big);
    try self.writer.interface.writeInt(i32, 80877102, .big);
    try self.writer.interface.writeInt(i32, process_id, .big);
    try self.writer.interface.writeAll(secret_key);
    try self.writer.interface.flush();
}

pub fn terminate(self: *Connection,) !void {
    try self.writer.interface.writeByte('X');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
    self.close();
}
