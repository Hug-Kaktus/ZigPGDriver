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
const TypedQueryResult = types.TypedQueryResult;
const PreparedStatement = types.PreparedStatement;
const BindedPreparedStatement = types.BindedPreparedStatement;
const PendingQuery = types.PendingQuery;

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
        param_types.items.len * 4;
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
) !*BindedPreparedStatement {
    try bindMsg(self, destination_portal, source_prepared_statement, parameter_format_codes, parameter_values, result_column_format_codes);
    try flush(self);
    var r = &self.reader;
    while (true) {
        const msg_type = try r.interface.takeByte();
        _ = try r.interface.takeInt(i32, .big);
        switch (msg_type) {
            '2' => {
                var binded_prepared_statement = try self.allocator.create(BindedPreparedStatement);
                binded_prepared_statement.prepared_statement = source_prepared_statement;
                binded_prepared_statement.portal_name = destination_portal;
                return binded_prepared_statement;
                // return &BindedPreparedStatement{
                //     .prepared_statement = source_prepared_statement,
                //     .portal_name = destination_portal,
                // };
            },
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
                std.debug.print("Unknown msg: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
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

pub fn close(
    self: *Connection,
    object_type: u8,
    name: []const u8,
) !void {
    try self.writer.interface.writeByte('C');
    try self.writer.interface.writeInt(i32, 4 + 1 + name.len + 1, .big);
    try self.writer.interface.writeByte(object_type);
    try self.writer.interface.writeAll(name);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
    var r = &self.reader;
    const msg_type = try r.interface.takeByte();
    _ = try r.interface.takeInt(i32, .big);
    switch (msg_type) {
        '3' => {
            std.debug.print("Close complete\n", .{});
            return;
        },
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
            std.debug.print("Unknown msg: {c}\n", .{msg_type});
            return error.UnknownMessageType;
        },
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
) !*PreparedStatement {
    defer parameter_types.deinit(self.allocator);

    const arena = try self.allocator.create(std.heap.ArenaAllocator);
    errdefer self.allocator.destroy(arena);

    arena.* = std.heap.ArenaAllocator.init(self.allocator);
    errdefer arena.deinit();

    // var arena = std.heap.ArenaAllocator.init(self.allocator);
    // errdefer arena.deinit();
    const arena_allocator = arena.allocator();
    var prepared_statement = try self.allocator.create(PreparedStatement);
    prepared_statement.arena = arena;
    prepared_statement.name = name;
    // var prepared_statement = PreparedStatement{
    //     .arena = arena,
    //     .name = name,
    //     .fields = undefined,
    //     .parameter_count = undefined,
    //     .parameters = undefined,
    // };

    try parseMsg(self, name, sql, parameter_types);
    try flush(self);

    var r = &self.reader;
    while (true) {
        const msg_type = try r.interface.takeByte();
        _ = try r.interface.takeInt(i32, .big);
        switch (msg_type) {
            '1' => {
                try describe(self, 'S', name);
                try flush(self);
            },
            'T' => {
                prepared_statement.fields = try parseFieldData(self, arena_allocator);
                return prepared_statement;
            },
            't' => {
                prepared_statement.parameters = try std.ArrayList(i32).initCapacity(arena_allocator, 4);
                prepared_statement.parameter_count = try r.interface.takeInt(i16, .big);
                for (0..@intCast(prepared_statement.parameter_count)) |_| {
                    try prepared_statement.parameters.append(arena_allocator, try r.interface.takeInt(i32, .big));
                }
            },
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
    var r = &self.reader;

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    const a = arena.allocator();
    var rows = try std.ArrayList(Row).initCapacity(a, 8);

    while (true) {
        const msg_type = try r.interface.takeByte();
        _ = try r.interface.takeInt(i32, .big);
        switch (msg_type) {
            'D' => {
                const row = try parseRowData(self, a, binded_prepared_statement.prepared_statement.fields.items);
                try rows.append(a, row);
            },
            'C' => {
                _ = try r.interface.takeDelimiter(0);
            },
            'Z' => {
                _ = try r.interface.takeByte();
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
                var error_message = try buildMessage(a, r);
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
) !TypedQueryResult(T) {
    var result = try executeQuery(self, binded_prepared_statement, max_rows_number);

    const arena_allocator = result.arena.allocator();

    var out = try std.ArrayList(T).initCapacity(arena_allocator, 8);
    for (result.rows.items) |row| {
        const obj = try decodeRowToStruct(arena_allocator, T, row, result.fields.items);
        try out.append(arena_allocator, obj);
    }
    return TypedQueryResult(T){
        .arena = result.arena,
        .rows = out,
    };
}

pub fn sendStatement(self: *Connection, prepared_statement: PreparedStatement, params: std.ArrayList(ParameterValue)) !void {
    try bindMsg(
        self,
        "",
        prepared_statement.name,
        std.ArrayList(i32).initCapacity(self.allocator, 0),
        params,
        std.ArrayList(i16).initCapacity(self.allocator, 0),
    );
    try executeMsg(self, "", 0);
}

pub fn flushPipeline(self: *Connection) !void {
    try sync(self);
}

pub fn readPipeline(self: *Connection) !void {
    var r = &self.reader;
    var current_query_index = 0;
    var in_error_recovery = false;
    while (current_query_index < self.pending.items.len) {
        const msg_type = try r.interface.takeByte();
        const msg_len = try r.interface.takeInt(i32, .big);

        if (in_error_recovery) {
            if (msg_type == 'Z') {
                _ = try r.interface.takeByte();
                for (self.pending.items[current_query_index..]) |*q| {
                    if (q.state == .pending) {
                        q.state = .skipped;
                    }
                }
                break;
            } else {
                try r.interface.take(msg_len - 4);
                continue;
            }
        }

        var current = &self.pending.items[current_query_index];
        switch (msg_type) {
            '1' => try r.interface.take(msg_len - 4),
            '2' => try r.interface.take(msg_len - 4),
            'D' => {
                if (current.prepared_statement.fields) |f| {
                    const row = try parseRowData(self, f.items);
                    try current.rows.append(self.allocator, row);
                } else {
                    return error.ProtocolError;
                }
            },
            'C' => {
                try r.interface.take(msg_len - 4);
                current.state = .done;
            },
            'Z' => {
                _ = try r.interface.takeByte();
                if (current.state == .pending) {
                    current.state = .done;
                }
                current_query_index += 1;
            },
            'E' => {
                var error_message = try buildMessage(self.allocator, r);
                defer error_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{error_message.items});
                current.error_message = error_message.items;
                current.state = .failed;
                in_error_recovery = true;
            },
            'N' => {
                var notice_message = try buildMessage(self.allocator, r);
                defer notice_message.deinit(self.allocator);
                std.debug.print("{s}\n", .{notice_message.items});
            },
            else => {
                std.debug.print("Unknown msg type: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn cancel(self: *Connection, process_id: i32, secret_key: []const u8) !void {
    try self.writer.interface.writeInt(i32, 8 + @as(i32, @intCast(secret_key.len)), .big);
    try self.writer.interface.writeInt(i32, 80877102, .big);
    try self.writer.interface.writeInt(i32, process_id, .big);
    try self.writer.interface.writeAll(secret_key);
    try self.writer.interface.flush();
}

pub fn terminate(
    self: *Connection,
) !void {
    try self.writer.interface.writeByte('X');
    try self.writer.interface.writeInt(i32, 4, .big);
    try self.writer.interface.flush();
    self.close();
}
