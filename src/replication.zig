const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const query = @import("query.zig");
const queryTyped = query.queryTyped;
const queryUntyped = query.queryUntyped;
const types = @import("types.zig");
const TypedQueryResult = types.TypedQueryResult;
const extended_query = @import("extended_query.zig");
const ParameterValue = extended_query.ParameterValue;

pub const IdentifySystemResponse = struct {
    systemid: []const u8,
    timeline: i64,
    xlogpos: []const u8,
    dbname: []const u8,

    pub fn show(self: *const IdentifySystemResponse) void {
        std.debug.print("systemid: {s}, timeline: {d}, xlogpos: {s}, dbname: {s}\n", .{ self.systemid, self.timeline, self.xlogpos, self.dbname });
    }
};

pub const TimelineHistoryResponse = struct {
    filename: []const u8,
    content: []const u8,

    pub fn show(self: *const TimelineHistoryResponse) void {
        std.debug.print("filename: {s}, content: {s}\n", .{ self.filename, self.content });
    }
};

pub fn identifySystem(self: *Connection) !IdentifySystemResponse {
    var query_result = try queryTyped(self, IdentifySystemResponse, "IDENTIFY_SYSTEM");
    defer query_result.deinit(self.allocator);
    return self.allocator.dupe(IdentifySystemResponse, query_result.rows.items[0]);
}

pub fn showParam(
    self: *Connection,
    name: []const u8,
) ![]const u8 {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 5 + name.len);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "SHOW ");
    try sql.appendSlice(self.allocator, name);
    return (try queryUntyped(self, sql.items)).rows.items[0].values[0].Text;
}

pub fn timelineHistory(
    self: *Connection,
    tli: []const u8,
) !TimelineHistoryResponse {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 17 + tli.len);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "TIMELINE_HISTORY ");
    try sql.appendSlice(self.allocator, tli);
    var query_result = try queryTyped(self, TimelineHistoryResponse, sql.items);
    query_result.deinit();
    return self.allocator.dupe(query_result.rows.items[0]);
}

const ReplicationType = enum {
    physical,
    logical,
};

const SnapshotType = enum {
    exp,
    use,
    nothing,
};

pub const CreateReplicationSlotResponse = struct {
    slot_name: []const u8,
    consistent_point: []const u8,
    snapshot_name: ?[]const u8,
    output_plugin: ?[]const u8,

    pub fn show(self: *const CreateReplicationSlotResponse) void {
        std.debug.print("===== CreateReplicationSlotResponse =====\n", .{});
        std.debug.print("Slot name: {s}\n", .{self.slot_name});
        std.debug.print("Consistent point: {s}\n", .{self.consistent_point});
        std.debug.print("Snapshot name: {?s}\n", .{self.snapshot_name});
        std.debug.print("Output plugin: {?s}\n", .{self.output_plugin});
    }
};

pub fn createReplicationSlotAdvanced(
    self: *Connection,
    slot_name: []const u8,
    temporary: bool,
    replication_type: ReplicationType,
    output_plugin: ?[]const u8,
    two_phase: bool,
    reserve_wal: bool,
    snapshot: SnapshotType,
    failover: bool,
) !TypedQueryResult(CreateReplicationSlotResponse) {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 256);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "CREATE_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    if (temporary) {
        try sql.appendSlice(self.allocator, " TEMPORARY");
    }
    switch (replication_type) {
        .physical => {
            try sql.appendSlice(self.allocator, " PHYSICAL");
        },
        .logical => {
            if (output_plugin) |op| {
                try sql.appendSlice(self.allocator, " LOGICAL ");
                try sql.appendSlice(self.allocator, op);
            }
        },
    }
    if (two_phase) {
        try sql.appendSlice(self.allocator, " TWO_PHASE");
    }
    if (reserve_wal) {
        try sql.appendSlice(self.allocator, " RESERVE_WAL");
    }
    _ = snapshot;
    // if (replication_type == .logical) {
    //     try sql.appendSlice(self.allocator, " SNAPSHOT ");
    //     switch (snapshot) {
    //         .exp => {
    //             try sql.appendSlice(self.allocator, "export");
    //         },
    //         .use => {
    //             try sql.appendSlice(self.allocator, "use");
    //         },
    //         .nothing => {
    //             try sql.appendSlice(self.allocator, "nothing");
    //         },
    //     }
    // }
    if (failover) {
        try sql.appendSlice(self.allocator, " FAILOVER");
    }
    const query_result = try queryTyped(self, CreateReplicationSlotResponse, sql.items);
    // return (try queryTyped(self, CreateReplicationSlotResponse, sql.items)).items[0];
    return query_result;
}

pub fn createPhysicalReplicationSlot(self: *Connection, slot_name: []const u8) !CreateReplicationSlotResponse {
    return try createReplicationSlotAdvanced(self, slot_name, false, .physical, null, false, false, .nothing, false);
}

pub fn createLogicalReplicationSlot(
    self: *Connection,
    slot_name: []const u8,
    output_plugin: []const u8,
) !TypedQueryResult(CreateReplicationSlotResponse) {
    return try createReplicationSlotAdvanced(self, slot_name, false, .logical, output_plugin, false, false, .exp, false);
}

pub fn alterReplicationSlot(
    self: *Connection,
    slot_name: []const u8,
    two_phase: bool,
    failover: bool,
) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "ALTER_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    try sql.appendSlice(self.allocator, " (");
    if (two_phase) {
        try sql.appendSlice(self.allocator, "TWO_PHASE true");
    } else {
        try sql.appendSlice(self.allocator, "TWO_PHASE false");
    }
    if (failover) {
        try sql.appendSlice(self.allocator, ", FAILOVER true");
    } else {
        try sql.appendSlice(self.allocator, ", FAILOVER false");
    }
    try sql.append(self.allocator, ')');
    std.debug.print("sql:\n{s}\n", .{sql.items});
    var query_result = try queryUntyped(self, sql.items);
    defer query_result.deinit(self.allocator);

    for (query_result.fields.items) |field| {
        field.show();
    }
}

pub const ReadReplicationSlotResponse = struct {
    slot_type: ?[]const u8,
    restart_lsn: ?[]const u8,
    restart_tli: ?i64,

    pub fn show(self: *const ReadReplicationSlotResponse) void {
        std.debug.print("===== ReadReplicationSlotResponse =====\n", .{});
        std.debug.print("slot_type: {?s}\n", .{self.slot_type});
        std.debug.print("restart_lsn: {?s}\n", .{self.restart_lsn});
        std.debug.print("restart_tli: {?d}\n", .{self.restart_tli});
    }
};

pub fn readReplicationSlot(self: *Connection, slot_name: []const u8) !TypedQueryResult(ReadReplicationSlotResponse) {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "READ_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    const query_result = try queryTyped(self, ReadReplicationSlotResponse, sql.items);
    if (query_result.rows.items.len > 0) {
        return query_result;
    } else {
        return error.ReadingTemporaryReplicationSlot;
    }
}

pub const PluginOption = struct {
    name: []const u8,
    value: ?[]const u8,
};

pub fn createPublication(self: *Connection, name: []const u8) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 64);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "CREATE PUBLICATION ");
    try sql.appendSlice(self.allocator, name);
    try sql.appendSlice(self.allocator, " FOR ALL TABLES");
    var query_result = try queryUntyped(self, sql.items);
    query_result.deinit(self.allocator);
}

pub fn dropPublication(self: *Connection, name: []const u8) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 64);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "DROP PUBLICATION ");
    try sql.appendSlice(self.allocator, name);
    var query_result = try queryUntyped(self, sql.items);
    query_result.deinit(self.allocator);
}

pub fn startLogicalReplication(
    self: *Connection,
    slot_name: []const u8,
    start_lsn: []const u8,
    plugin_options: ?std.ArrayList(PluginOption),
) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "START_REPLICATION SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    try sql.appendSlice(self.allocator, " LOGICAL ");
    try sql.appendSlice(self.allocator, start_lsn);
    if (plugin_options) |po| {
        defer po.deinit(self.allocator);
        try sql.appendSlice(self.allocator, " (");
        for (po.items, 0..) |plugin_option, i| {
            try sql.appendSlice(self.allocator, plugin_option.name);
            if (plugin_option.value) |v| {
                try sql.appendSlice(self.allocator, " '");
                try sql.appendSlice(self.allocator, v);
                try sql.append(self.allocator, '\'');
                if (std.mem.eql(u8, plugin_option.name, "streaming") and std.mem.eql(u8, v, "true")) {
                    self.streaming = true;
                }
            }
            if (i < po.items.len - 1) {
                try sql.appendSlice(self.allocator, ", ");
            }
        }
        try sql.append(self.allocator, ')');
    }
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + @as(i32, @intCast(sql.items.len)) + 1, .big);
    try self.writer.interface.writeAll(sql.items);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();

    var r = &self.reader;
    const cwd = std.Io.Dir.cwd();
    const file = try cwd.createFile("out.txt", .{});
    defer file.close();
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(self.io, &buffer);

    while (true) {
        const msg_type = try r.interface.takeByte();
        std.debug.print("msg_type: {c}\n", .{msg_type});
        const payload_len = try r.interface.takeInt(i32, .big) - 4;
        switch (msg_type) {
            'W' => {
                const copy_format = try r.interface.takeInt(i8, .big);
                std.debug.print("copy_format: {d}\n", .{copy_format});
                const columns_number = try r.interface.takeInt(i16, .big);
                var column_format_codes = try std.ArrayList(i16).initCapacity(self.allocator, @intCast(columns_number));
                for (0..@intCast(columns_number)) |_| {
                    try column_format_codes.append(self.allocator, try r.interface.takeInt(i16, .big));
                }
                for (column_format_codes.items) |code| {
                    std.debug.print("code: {d}\n", .{code});
                }
            },
            'd' => {
                const inner_msg_type = try r.takeByte();
                std.debug.print("msg_type: {c}\n", .{inner_msg_type});
                switch (inner_msg_type) {
                    'k' => {
                        try handleKeepalive(self);
                    },
                    'w' => {
                        try handleXLogData(self, payload_len - 1, &file_writer);
                    },
                    else => {
                        std.debug.print("Unsupported message type {c}\n", .{msg_type});
                        return error.UnknownMessageType;
                    },
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
                std.debug.print("Unsupported message type {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
    try file_writer.interface.flush();
}

pub fn parseTupleData(
    self: *Connection,
    writer: anytype,
) !void {
    var r = &self.reader;
    const columns_number = try r.interface.takeInt(i16, .big);
    std.debug.print("columns_number: {d}\n", .{columns_number});
    for (0..@intCast(columns_number)) |i| {
        try writer.interface.print("    Tuple{d}\n", .{i});
        try writer.interface.print("    Data format: {c}\n", .{try r.interface.takeByte()});
        const value_len = try r.interface.takeInt(i32, .big);
        try writer.interface.print("    Value: {any}\n", .{try r.interface.take(@intCast(value_len))});
    }
}

// LEAK? Probably no
pub fn LsnToString(allocator: std.mem.Allocator, lsn: i64) ![]const u8 {
    const upper: u32 = @intCast(lsn >> 32);
    const lower: u32 = @intCast(lsn & 0xffffffff);
    var string = try std.ArrayList(u8).initCapacity(allocator, 16);
    try string.print(allocator, "{X}/{X}", .{ upper, lower });
    return string.items;
}

pub fn handleXLogData(self: *Connection, size: i32, writer: anytype) !void {
    var r = &self.reader;

    const start = try r.interface.takeInt(u64, .big);
    const end = try r.interface.takeInt(u64, .big);
    const timestamp = try r.interface.takeInt(i64, .big);
    _ = timestamp;

    const payload_size = size - 8 - 8 - 8;
    const replication_msg_type = try r.interface.takeByte();
    std.debug.print("replication_msg_type: {c}\n", .{replication_msg_type});

    switch (replication_msg_type) {
        'B' => {
            try writer.interface.writeAll("=Begin\n");
            const transaction_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction LSN: {s}\n", .{transaction_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
        },
        'M' => {
            try writer.interface.writeAll("=Message\n");
            try writer.interface.flush();
        },
        'C' => {
            try writer.interface.writeAll("=Commit\n");
            _ = try r.takeByte();
            const commit_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Commit LSN: {s}\n", .{commit_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            const end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("End LSN: {s}\n", .{end_lsn});
            try writer.interface.flush();
        },
        'O' => {
            try writer.interface.writeAll("=Origin\n");
            const commit_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Commit LSN on the origin server: {s}\n", .{commit_lsn});
            try writer.interface.print("Origin name: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'R' => {
            try writer.interface.writeAll("=Relation\n");
            try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Namespace: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.print("Relation name: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.print("Replica identity: {d}\n", .{try r.interface.takeInt(i8, .big)});
            const columns_number = try r.takeInt(i16, .big);
            for (0..@intCast(columns_number)) |i| {
                try writer.interface.print("Column{d}\n", .{i});
                try writer.interface.print("    Column flags: {d}\n", .{try r.interface.takeInt(i8, .big)});
                try writer.interface.print("    Column name: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
                try writer.interface.print("    Column OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
                try writer.interface.print("    Type modifier: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.flush();
        },
        'Y' => {
            try writer.interface.writeAll("=Type\n");
            if (self.streaming) {
                try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Namespace: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.print("Data type name: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'I' => {
            try writer.interface.writeAll("=Insert\n");
            if (self.streaming) {
                try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            const byte = try r.interface.takeByte();
            std.debug.print("byte: {d}\n", .{byte});
            try parseTupleData(self, writer);
            try writer.interface.flush();
        },
        'U' => {
            try writer.interface.writeAll("=Update\n");
            if (self.streaming) {
                try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            const submsg_type = try r.interface.peekByte();
            switch (submsg_type) {
                'K' => {
                    try parseTupleData(self, writer);
                },
                'O' => {
                    try parseTupleData(self, writer);
                },
                else => {},
            }
            r.interface.toss(1);
            _ = try r.interface.takeByte();
            try parseTupleData(self, writer);
            try writer.interface.flush();
        },
        'D' => {
            try writer.interface.writeAll("=Delete\n");
            if (self.streaming) {
                try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            const submsg_type = try r.interface.takeByte();
            switch (submsg_type) {
                'K' => {
                    try parseTupleData(self, writer);
                },
                'O' => {
                    try parseTupleData(self, writer);
                },
                else => return error.UnexpectedByte,
            }
            try writer.interface.flush();
        },
        'T' => {
            try writer.interface.writeAll("=Truncate\n");
            if (self.streaming) {
                try writer.interface.print("Transaction id: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            const relations_number = try r.takeInt(i32, .big);
            try writer.interface.print("Option bits: {d}\n", .{try r.interface.takeInt(i32, .big)});

            for (0..@intCast(relations_number)) |_| {
                try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            }
            try writer.interface.print("Relation OID: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.flush();
        },
        'S' => {
            try writer.interface.writeAll("=Stream Start\n");
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Is first stream segment: {d}\n", .{try r.interface.takeInt(i8, .big)});
            try writer.interface.flush();
        },
        'E' => {
            try writer.interface.writeAll("=Stream Stop\n");
            try writer.interface.flush();
        },
        'c' => {
            try writer.interface.writeAll("=Stream Commit\n");
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            _ = try r.takeByte();
            const commit_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Commit LSN: {s}\n", .{commit_lsn});
            const transaction_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction end LSN: {s}\n", .{transaction_end_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.flush();
        },
        'A' => {
            try writer.interface.writeAll("=Stream Abort\n");
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Subtransaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            if (self.parallel) {
                const operation_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
                try writer.interface.print("Operation LSN: {s}\n", .{operation_lsn});
            }
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.flush();
        },
        'b' => {
            try writer.interface.writeAll("=Begin Prepare\n");
            const prepare_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Prepare LSN: {s}\n", .{prepare_lsn});
            const transaction_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction end LSN: {s}\n", .{transaction_end_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Prepared transaction GID: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'P' => {
            try writer.interface.writeAll("=Prepare\n");
            _ = try r.takeByte();
            const prepare_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Prepare LSN: {s}\n", .{prepare_lsn});
            const transaction_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction end LSN: {s}\n", .{transaction_end_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Prepared transaction GID: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'K' => {
            try writer.interface.writeAll("=Commit Prepared\n");
            _ = try r.takeByte();
            const commit_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction commit LSN: {s}\n", .{commit_lsn});
            const commit_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction commit end LSN: {s}\n", .{commit_end_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Prepared transaction GID: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'r' => {
            try writer.interface.writeAll("=Rollback Prepared\n");
            const transaction_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Transaction commit end LSN: {s}\n", .{transaction_end_lsn});
            const rollback_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Rollback end LSN: {s}\n", .{rollback_end_lsn});
            try writer.interface.print("Prepare timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Rollback timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Prepared transaction GID: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        'p' => {
            try writer.interface.writeAll("=Stream Prepare\n");
            _ = try r.takeByte();
            const prepare_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Prepare LSN: {s}\n", .{prepare_lsn});
            const transaction_end_lsn = try LsnToString(self.allocator, try r.interface.takeInt(i64, .big));
            try writer.interface.print("Prepared transaction end LSN: {s}\n", .{transaction_end_lsn});
            try writer.interface.print("Timestamp: {d}\n", .{try r.interface.takeInt(i64, .big)});
            try writer.interface.print("Transaction xid: {d}\n", .{try r.interface.takeInt(i32, .big)});
            try writer.interface.print("Prepared transaction GID: {s}\n", .{(try r.interface.takeDelimiter(0)).?});
            try writer.interface.flush();
        },
        else => {
            std.debug.print("Unsupported message type {c}\n", .{replication_msg_type});
            return error.UnknownMessageType;
        },
    }

    std.debug.print("WAL: start={} end={} size={}\n", .{ start, end, payload_size });
}

pub fn handleKeepalive(self: *Connection) !void {
    var r = &self.reader;

    const end = try r.interface.takeInt(u64, .big);
    const timestamp = try r.interface.takeInt(i64, .big);
    const reply_requested = try r.interface.takeByte();
    _ = timestamp;

    std.debug.print("Keepalive: lsn={} reply={}\n", .{ end, reply_requested });

    if (reply_requested == 1) {
        try sendStandbyStatus(self, end);
    }
}

pub fn sendStandbyStatus(self: *Connection, lsn: u64) !void {
    try self.writer.interface.writeByte('d');
    try self.writer.interface.writeInt(i32, 4 + 1 + 8 + 8 + 8 + 8 + 1, .big);
    try self.writer.interface.writeByte('r');
    try self.writer.interface.writeInt(u64, lsn, .big);
    try self.writer.interface.writeInt(u64, lsn, .big);
    try self.writer.interface.writeInt(u64, lsn, .big);
    try self.writer.interface.writeInt(i64, 0, .big);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn dropReplicationSlot(
    self: *Connection,
    slot_name: []const u8,
    wait: bool,
) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 64);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "DROP_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    if (wait) {
        try sql.appendSlice(self.allocator, " WAIT");
    }
    var query_result = try queryUntyped(self, sql.items);
    defer query_result.deinit(self.allocator);
    for (query_result.fields.items) |field| {
        field.show();
    }
}

pub fn uploadManifest(self: *Connection) !void {
    _ = try queryUntyped(self, "UPLOAD_MANIFEST");
}

pub const Target = enum {
    client,
    server,
    blackhole,
};

pub const TargetDetail = struct {
    backup_directory: []const u8,
};

pub const Checkpoint = enum {
    fast,
    spread,
};

pub const CompressionMethod = enum {
    gzip,
    lz4,
    zstd,
};

pub const ManifestOption = enum {
    yes,
    force_encode,
    no,
};

pub const ManifestChecksumAlgorithm = enum {
    none,
    crc32c,
    sha224,
    sha256,
    sha384,
    sha512,
};

pub const CompressionDetailKeywords = struct {
    level: ?i32,
    long: ?bool,
    workers: ?u32,
};

pub const CompressionDetail = union {
    int: i32,
    keywords: CompressionDetailKeywords,
};

pub const BaseBackup = struct {
    label: ?[]const u8,
    target: ?Target,
    target_detail: ?[]const u8,
    progress: ?bool,
    checkpoint: ?Checkpoint,
    wal: ?bool,
    wait: ?bool,
    compression: ?CompressionMethod,
    compression_detail: ?CompressionDetail,
    max_rate: ?i32,
    tablespace_map: ?bool,
    verify_checksums: ?bool,
    manifest: ?ManifestOption,
    manifest_checksums: ?ManifestChecksumAlgorithm,
    incremental: ?bool,
};

pub const BackupFirstResponse = struct {
    start_pos: u64,
    tid: u64,
};

pub const BackupSecondResponse = struct {
    spcoid: u32,
    spclocation: []const u8,
    size: i64,
};

pub fn intToString(allocator: std.mem.Allocator, comptime T: type, x: T) !std.ArrayList(u8) {
    var string = try std.ArrayList(u8).initCapacity(allocator, 8);
    if (x < 0) {
        string.append(allocator, '-');
        x = -x;
    }
    std.debug.print("x: {d}\n", .{x});
    var dividend_not_zero = true;
    var remainder: u8 = undefined;
    var i: usize = 1;
    while (dividend_not_zero) : (i += 1) {
        remainder = @intCast(x % std.math.pow(usize, 10, i));
        x -= remainder;
        dividend_not_zero = x != 0;
        string.append(allocator, remainder / std.fmt.digitToChar(std.math.pow(usize, 10, i - 1), .lower));
    }
    return string;
}

pub const Tablespace = struct {
    spcoid: u32,
    spclocation: []const u8,
    size: ?i64,
    pub fn show(self: *const Tablespace) void {
        std.debug.print("===== Tablespace =====\n", .{});
        std.debug.print("spcoid: {d}\n", .{self.spcoid});
        std.debug.print("spclocation: {s}\n", .{self.spclocation});
        std.debug.print("spcoid: {?d}\n", .{self.size});
    }
};

pub fn readBackupResponses(self: *Connection) !void {
    var r = &self.reader;
    // var tablespaces = try std.ArrayList(Tablespace).initCapacity(self.allocator, 8);
    // for (tablespaces.items) |tablespace| {
    //     tablespace.show();
    // }
    // const start_pos = reader.takeInt(i64, .big);
    // const tid = reader.takeInt(i64, .big);
    while (true) {
        const msg_type = try r.interface.takeByte();
        std.debug.print("msg_type: {c}\n", .{msg_type});
        _ = try r.interface.takeInt(i32, .big);
        switch (msg_type) {
            'n' => {
                const archive_name = (try r.interface.takeDelimiter(0)).?;
                const directory = (try r.interface.takeDelimiter(0)).?;
                std.debug.print("archive_name: {s} directory: {s}\n", .{ archive_name, directory });
            },
            'm' => {
                std.debug.print("Backup manifest started\n", .{});
            },
            'd' => {
                // const data = try reader.take(data_len);
            },
            'p' => {
                const processed_bytes = try r.interface.takeInt(i64, .big);
                _ = processed_bytes;
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
                std.debug.print("Unsupported message type {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn baseBackupAdvanced(
    self: *Connection,
    label: ?[]const u8,
    target: ?Target,
    target_detail: ?TargetDetail,
    progress: ?bool,
    checkpoint: ?Checkpoint,
    wal: ?bool,
    wait: ?bool,
    compression: ?CompressionMethod,
    compression_detail: ?CompressionDetail,
    max_rate: ?i32,
    tablespace_map: ?bool,
    verify_checksums: ?bool,
    manifest: ?ManifestOption,
    manifest_checksums: ?ManifestChecksumAlgorithm,
    incremental: ?bool,
) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 64);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "BASE_BACKUP");
    if (label) |label_payload| {
        try sql.appendSlice(self.allocator, " LABEL ");
        try sql.appendSlice(self.allocator, label_payload);
    }
    if (target) |target_payload| {
        try sql.appendSlice(self.allocator, " TARGET ");
        switch (target_payload) {
            .client => {
                try sql.appendSlice(self.allocator, "'client'");
            },
            .server => {
                try sql.appendSlice(self.allocator, "'server'");
            },
            .blackhole => {
                try sql.appendSlice(self.allocator, "'blackhole'");
            },
        }
    }
    if (target_detail) |target_detail_payload| {
        try sql.appendSlice(self.allocator, " TARGET_DETAIL ");
        try sql.appendSlice(self.allocator, target_detail_payload.backup_directory);
    }
    if (progress) |progress_payload| {
        if (progress_payload) {
            try sql.appendSlice(self.allocator, " PROGRESS");
        }
    }
    if (checkpoint) |checkpoint_payload| {
        try sql.appendSlice(self.allocator, " CHECKPOINT ");
        switch (checkpoint_payload) {
            .fast => {
                try sql.appendSlice(self.allocator, "'fast'");
            },
            .spread => {
                try sql.appendSlice(self.allocator, "'spread'");
            },
        }
    }
    if (wal) |wal_payload| {
        if (wal_payload) {
            try sql.appendSlice(self.allocator, " WAL");
        }
    }
    if (wait) |wait_payload| {
        if (wait_payload) {
            try sql.appendSlice(self.allocator, " WAIT");
        }
    }
    if (compression) |compression_payload| {
        try sql.appendSlice(self.allocator, " COMPRESSION '");
        std.debug.print("@tagName(compression_payload): {any}\n", .{@tagName(compression_payload)});
        try sql.appendSlice(self.allocator, @tagName(compression_payload));
        try sql.append(self.allocator, '\'');
    }
    if (compression_detail) |compression_detail_payload| {
        switch (compression_detail_payload) {
            .int => {
                try sql.appendSlice(self.allocator, compression_detail_payload);
            },
            .keywords => |x| {
                try sql.append(self.allocator, '(');
                if (x.level) |level_payload| {
                    try sql.appendSlice(self.allocator, "level=");
                    // POTENTIAL MEMORY LEAK
                    try sql.appendSlice(self.allocator, intToString(self.allocator, i32, level_payload).items);
                }
                if (x.long) |long_payload| {
                    if (long_payload) {
                        try sql.appendSlice(self.allocator, "long=");
                        try sql.appendSlice(self.allocator, "true");
                    }
                }
                if (x.workers) |workers_payload| {
                    try sql.appendSlice(self.allocator, "workers=");
                    // POTENTIAL MEMORY LEAK
                    try sql.appendSlice(self.allocator, intToString(self.allocator, u32, workers_payload).items);
                }
                try sql.append(self.allocator, ')');
            },
        }
    }
    if (max_rate) |max_rate_payload| {
        try sql.appendSlice(self.allocator, " MAX_RATE ");
        // POTENTIAL MEMORY LEAK
        try sql.appendSlice(self.allocator, intToString(self.allocator, i32, max_rate_payload).items);
    }
    if (tablespace_map) |tablespace_map_payload| {
        if (tablespace_map_payload) {
            try sql.appendSlice(self.allocator, " TABLESPACE_MAP");
        }
    }
    if (verify_checksums) |verify_checksums_payload| {
        if (verify_checksums_payload) {
            try sql.appendSlice(self.allocator, " VERIFY_CHECKSUMS");
        }
    }
    if (manifest) |manifest_payload| {
        try sql.appendSlice(self.allocator, " MANIFEST ");
        switch (manifest_payload) {
            .yes => {
                try sql.appendSlice(self.allocator, "'yes'");
            },
            .force_encode => {
                try sql.appendSlice(self.allocator, "'force_encode'");
            },
            .no => {
                try sql.appendSlice(self.allocator, "'no'");
            },
        }
    }
    if (manifest_checksums) |manifest_checksums_payload| {
        try sql.appendSlice(self.allocator, " MANIFEST_CHECKSUMS '");
        std.debug.print("@tagName(manifest_checksums_payload): {any}\n", .{@tagName(manifest_checksums_payload)});
        try sql.appendSlice(self.allocator, @tagName(manifest_checksums_payload));
        try sql.append(self.allocator, '\'');
    }
    if (incremental) |incremental_payload| {
        if (incremental_payload) {
            try sql.appendSlice(self.allocator, " INCREMENTAL");
        }
    }
    try readBackupResponses(self);
}

pub fn baseBackup(self: *Connection, bb: BaseBackup) !void {
    baseBackupAdvanced(
        self,
        bb.label,
        bb.target,
        bb.target_detail,
        bb.progress,
        bb.checkpoint,
        bb.wal,
        bb.wait,
        bb.compression,
        bb.compression_detail,
        bb.max_rate,
        bb.tablespace_map,
        bb.verify_checksums,
        bb.manifest,
        bb.manifest_checksums,
        bb.incremental,
    );
    try readBackupResponses(self);
}
