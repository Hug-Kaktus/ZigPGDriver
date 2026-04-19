const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const query = @import("query.zig");
const queryTyped = query.queryTyped;
const queryWithFields = query.queryWithFields;
const extended_query = @import("extended_query.zig");
const ParameterValue = extended_query.ParameterValue;

pub const Lsn = struct {
    value: u64,

    pub fn fromParts(high: u32, low: u32) Lsn {
        return .{ .value = (@as(u64, high) << 32) | low};
    }
};

pub const LogicalStream = struct {
    conn: *Connection,
    buffer: []const u8,

    last_lsn: Lsn,
    ack_lsn: Lsn,
};

pub const Message = union(enum) {
    begin: Begin,
    commit: Commit,
    relation: Relation,
    insert: Insert,
    update: Update,
    delete: Delete,
};

pub const Begin = struct {
    final_lsn: Lsn,
    commit_time: i64,
    xid: u32,
};

pub const Commit = struct {
    lsn: Lsn,
    end_lsn: Lsn,
    commit_time: i64,
};

pub const Relation = struct {
    id: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,

    columns: []const Column,

    pub const Column = struct {
        flags: u8,
        name: []const u8,
        type_oid: u32,
        atttypmod: i32,
    };
};

pub const Tuple = struct {
    columns: []const ColumnValue,

    pub const ColumnValue = union(enum) {
        null,
        unchanged_toast,
        text: []const u8,
    };
};

pub const Insert = struct {
    relation_id: u32,
    tuple: Tuple,
};

pub const Update = struct {
    relation_id: u32,
    old: ?Tuple,
    new: Tuple,
};

pub const Delete = struct {
    relation_id: u32,
    old: Tuple,
};

pub const IdentifySystemResponse = struct {
    systemid: []const u8,
    timeline: i64,
    xlogpos: []const u8,
    dbname: []const u8,

    pub fn show(self: *const IdentifySystemResponse) void {
        std.debug.print("systemid: {s}, timeline: {d}, xlogpos: {s}, dbname: {s}\n", .{self.systemid,
            self.timeline, self.xlogpos, self.dbname});
    }
};

pub const TimelineHistoryResponse = struct {
    filename: []const u8,
    content: []const u8,

    pub fn show(self: *const TimelineHistoryResponse) void {
        std.debug.print("filename: {s}, content: {s}\n", .{self.filename, self.content});
    }
};

pub fn identifySystem(self: *Connection) !IdentifySystemResponse {
    return (try queryTyped(self, IdentifySystemResponse, "IDENTIFY_SYSTEM")).items[0];
}

pub fn showParam(self: *Connection, name: []const u8) ![]const u8 {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 5+name.len);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "SHOW ");
    try sql.appendSlice(self.allocator, name);
    return (try queryWithFields(self, sql.items)).rows.items[0].values[0].Text;
}

pub fn timelineHistory(self: *Connection, tli: []const u8) !TimelineHistoryResponse {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 17+tli.len);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "TIMELINE_HISTORY ");
    try sql.appendSlice(self.allocator, tli);
    return (try queryTyped(self, TimelineHistoryResponse, sql.items)).items[0];
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
    snapshot_name: []const u8,
    output_plugin: []const u8,
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
) !CreateReplicationSlotResponse {
    try self.writer.interface.writeByte('Q');
    var msg_len: i32 = 24 + @as(i32, @intCast(slot_name.len));
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "CREATE_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    if (temporary) {
        try sql.appendSlice(self.allocator, " TEMPORARY");
        msg_len += 10;
    }
    switch (replication_type) {
        .physical => {
            try sql.appendSlice(self.allocator, " PHYSICAL");
            msg_len += 9;
        },
        .logical => {
            if (output_plugin) |op| {
                try sql.appendSlice(self.allocator, " LOGICAL ");
                try sql.appendSlice(self.allocator, op);
                msg_len += 9 + @as(i32, @intCast(op.len));
            }
        },
    }
    if (two_phase) {
        try sql.appendSlice(self.allocator, " TWO_PHASE");
        msg_len += 10;
    }
    if (reserve_wal) {
        try sql.appendSlice(self.allocator, " RESERVE_WAL");
        msg_len += 12;
    }
    try sql.appendSlice(self.allocator, " SNAPSHOT ");
    msg_len += 10;
    switch (snapshot) {
        .exp => {
            try sql.appendSlice(self.allocator, "'export'");
            msg_len += 8;
        },
        .use => {
            try sql.appendSlice(self.allocator, "'use'");
            msg_len += 5;
        },
        .nothing => {
            try sql.appendSlice(self.allocator, "'nothing'");
            msg_len += 9;
        },
    }
    if (failover) {
        try sql.appendSlice(self.allocator, " FAILOVER");
        msg_len += 9;
    }
    try self.writer.interface.writeInt(msg_len);
    return (try queryTyped(self, CreateReplicationSlotResponse, sql.items)).items[0];
}



pub fn startPhysicalReplication(
    self: *Connection,
    start_lsn: []const u8,
) !void {
    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, 4 + 18 + @as(i32, @intCast(start_lsn.len)) + 1);
    try self.writer.interface.writeAll("START_REPLICATION ");
    try self.writer.interface.writeAll(start_lsn);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn startLogicalReplication(
    self: *Connection,
    slot: []const u8,
    start_lsn: []const u8,
    publication_names: std.ArrayList([]const u8),
) !void {

    try self.writer.interface.writeByte('Q');
    const pub_names_len = blk: {
        var len = 0;
        for (publication_names.items) |pub_name| {
            len += pub_name.len + 1;
        }
        if (len > 0) {
            len -= 1;
        }
        break :blk len;
    };
    try self.writer.interface.writeInt(i32, 4 + 23 + @as(i32, @intCast(slot.len)) +
    9 + @as(i32, @intCast(start_lsn.len)) +
    40 + pub_names_len + 88 + 1, .big);

    try self.writer.interface.writeAll("START_REPLICATION SLOT ");
    try self.writer.interface.writeAll(slot);
    try self.writer.interface.writeAll(" LOGICAL ");
    try self.writer.interface.writeAll(start_lsn);
    try self.writer.interface.writeAll(" (proto_version '4', publication_names '");
    for (publication_names.items, 0..) |pub_name, i| {
        try self.writer.interface.writeAll(pub_name);
        if (i != publication_names.items.len-1) {
            try self.writer.interface.writeByte(",");
        }
    }
    try self.writer.interface.writeAll("', binary 'true', messages 'true', streaming 'parallel', two_phase 'true', origin 'any')");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn readReplicationStream(self: *Connection) !void {
    var reader = self.reader.interface();

    while (true) {
        const msg_type = try reader.takeByte();
        const msg_len = try reader.takeInt(i32, .big);

        switch (msg_type) {
            'W' => {
                const copy_format = reader.readInt(i8, .big);
                _ = copy_format;
                const format_codes_count = try reader.readInt(i16, .big);
                var format_codes = try std.ArrayList(i16).initCapacity(self.allocator, format_codes_count);
                for (0..@intCast(format_codes_count)) |_| {
                    format_codes.append(self.allocator, try reader.readInt(i16, .big));
                }
            },
            'd' => {
                handleCopyData(self, msg_len-4);
            },
            'c' => {
                return;
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
                std.debug.print("Unknown replication msg: {c}\n", .{msg_type});
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn handleCopyData(self: *Connection, size: i32) !void {
    var reader = self.reader.interface();

    const subtype = try reader.takeByte();

    switch (subtype) {
        'w' => {
            try handleXLogData(self, size - 1);
        },
        'k' => {
            try handleKeepalive(self, size - 1);
        },
        else => {
            std.debug.print("Unknown CopyData subtype: {c}\n", .{subtype});
            return error.UnknownMessageType;
        },
    }
}

pub fn handleXLogData(self: *Connection, size: i32) !void {
    var reader = self.reader.interface();

    const start = try reader.takeInt(u64, .big);
    const end = try reader.takeInt(u64, .big);
    const timestamp = try reader.takeInt(i64, .big);
    _ = timestamp;

    const payload_size = size - 8 - 8 - 8;

    var buf = try self.allocator.alloc(u8, @intCast(payload_size));
    defer self.allocator.free(buf);

    try reader.readNoEof(&buf);

    std.debug.print(
        "WAL: start={} end={} size={}\n",
        .{ start, end, payload_size }
    );
}

pub fn handleKeepalive(self: *Connection) !void {
    var reader = self.reader.interface();

    const end = try reader.takeInt(u64, .big);
    const timestamp = try reader.takeInt(i64, .big);
    const reply_requested = try reader.takeByte();
    _ = timestamp;

    std.debug.print(
        "Keepalive: lsn={} reply={}\n",
        .{ end, reply_requested }
    );

    if (reply_requested == 1) {
        try self.sendStandbyStatus(end);
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
