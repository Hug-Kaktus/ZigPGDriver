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
) !CreateReplicationSlotResponse {
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
    return (try queryTyped(self, CreateReplicationSlotResponse, sql.items)).items[0];
}

pub fn createPhysicalReplicationSlot(
    self: *Connection,
    slot_name: []const u8
    ) !CreateReplicationSlotResponse {
    return try createReplicationSlotAdvanced(self, slot_name, false, .physical, null, false, false, .nothing, false);
}

pub fn createLogicalReplicationSlot(
    self: *Connection,
    slot_name: []const u8,
    output_plugin: []const u8,
    ) !CreateReplicationSlotResponse {
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
    var query_result = try queryWithFields(self, sql.items);
    defer query_result.deinit();

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

pub fn readReplicationSlot(self: *Connection, slot_name: []const u8) !ReadReplicationSlotResponse {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "READ_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    const response = (try queryTyped(self, ReadReplicationSlotResponse, sql.items));
    if (response.items.len > 0) {
        return response.items[0];
    } else {
        return error.ReadingTemporaryReplicationSlot;
    }
}

// pub fn startPhysicalReplication(self: *Connection, slot_name: []const u8, start_lsn: []const u8, timeline: i64) !void {
// 
// } 

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
    _ = try queryWithFields(self, sql.items);
}

pub fn startLogicalReplication(
    self: *Connection,
    slot_name: []const u8,
    start_lsn: []const u8,
    plugin_options: ?std.ArrayList(PluginOption)
    ) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 128);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "START_REPLICATION SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    try sql.appendSlice(self.allocator, " LOGICAL ");
    try sql.appendSlice(self.allocator, start_lsn);
    if (plugin_options) |po| {
        try sql.appendSlice(self.allocator, " (");
        for (po.items, 0..) |plugin_option, i| {
            try sql.appendSlice(self.allocator, plugin_option.name);
            if (plugin_option.value) |v| {
                try sql.appendSlice(self.allocator, " '");
                try sql.appendSlice(self.allocator, v);
                try sql.append(self.allocator, '\'');
            }
            if (i < po.items.len-1) {
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

    var reader = self.reader.interface();
    while (true) {
        const msg_type = try reader.takeByte();
        std.debug.print("msg_type: {c}\n", .{msg_type});
        const payload_len = try reader.takeInt(i32, .big) - 4;
        switch (msg_type) {
            'W' => {
                const copy_format = try reader.takeInt(i8, .big);
                std.debug.print("copy_format: {d}\n", .{copy_format});
                const columns_number = try reader.takeInt(i16, .big);
                var column_format_codes = try std.ArrayList(i16).initCapacity(self.allocator, @intCast(columns_number));
                for (0..@intCast(columns_number)) |_| {
                    try column_format_codes.append(self.allocator, try reader.takeInt(i16, .big));
                }
                for (column_format_codes.items) |code| {
                    std.debug.print("code: {d}\n", .{code});
                }
            },
            'd' => {
                const inner_msg_type = try reader.takeByte();
                std.debug.print("msg_type: {c}\n", .{inner_msg_type});
                switch (inner_msg_type) {
                    'k' => {
                        try handleKeepalive(self);
                    },
                    'w' => {
                        try handleXLogData(self, payload_len-1);
                    },
                    else => {
                        std.debug.print("Unsupported message type {c}\n", .{msg_type});
                        return error.UnknownMessageType;
                    },
                }
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
                return error.UnknownMessageType;
            },
        }
    }
}

pub fn dropReplicationSlot(self: *Connection, slot_name: []const u8, wait: bool) !void {
    var sql = try std.ArrayList(u8).initCapacity(self.allocator, 64);
    defer sql.deinit(self.allocator);
    try sql.appendSlice(self.allocator, "DROP_REPLICATION_SLOT ");
    try sql.appendSlice(self.allocator, slot_name);
    if (wait) {
        try sql.appendSlice(self.allocator, " WAIT");
    }
    var query_result = try queryWithFields(self, sql.items);
    defer query_result.deinit();
    for (query_result.fields.items) |field| {
        field.show();
    }
}

pub fn uploadManifest(self: *Connection) !void {
    try queryWithFields(self, "UPLOAD_MANIFEST");
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
    incremental: ?bool
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
    while (dividend_not_zero): (i += 1) {
        remainder = @intCast(x % std.math.pow(usize, 10, i));
        x -= remainder;
        dividend_not_zero = x != 0;
        string.append(allocator, remainder / std.fmt.digitToChar(std.math.pow(usize, 10, i-1), .lower));
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
    var reader = self.reader.interface();
    // var tablespaces = try std.ArrayList(Tablespace).initCapacity(self.allocator, 8);
    // for (tablespaces.items) |tablespace| {
    //     tablespace.show();
    // }
    // const start_pos = reader.takeInt(i64, .big);
    // const tid = reader.takeInt(i64, .big);
    while (true) {
        const msg_type = try reader.takeByte();
        std.debug.print("msg_type: {c}\n", .{msg_type});
        _ = try self.reader.interface().takeInt(i32, .big);
        switch (msg_type) {
            'n' => {
                const archive_name = (try reader.takeDelimiter(0)).?;
                const directory = (try reader.takeDelimiter(0)).?;
                std.debug.print("archive_name: {s} directory: {s}\n", .{archive_name, directory});
            },
            'm' => {
                std.debug.print("Backup manifest started\n", .{});
            },
            'd' => {
                // const data = try reader.take(data_len);
            },
            'p' => {
                const processed_bytes = try reader.takeInt(i64, .big);
                _ = processed_bytes;
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
    incremental: ?bool
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
    baseBackupAdvanced(self, bb.label, bb.target, bb.target_detail, bb.progress,
        bb.checkpoint, bb.wal, bb.wait, bb.compression, bb.compression_detail,
        bb.max_rate, bb.tablespace_map, bb.verify_checksums, bb.manifest,
        bb.manifest_checksums, bb.incremental);
        try readBackupResponses(self);
}

pub fn handleXLogData(self: *Connection, size: i32) !void {
    var reader = self.reader.interface();

    const start = try reader.takeInt(u64, .big);
    const end = try reader.takeInt(u64, .big);
    const timestamp = try reader.takeInt(i64, .big);
    _ = timestamp;

    const payload_size = size - 8 - 8 - 8;
    const replication_msg_type = try reader.takeByte();
    std.debug.print("replication_msg_type: {c}", .{replication_msg_type});

    const buf = reader.take(@intCast(payload_size));
    std.debug.print("buf: {any}\n", .{buf});

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

    std.debug.print( "Keepalive: lsn={} reply={}\n", .{ end, reply_requested });

    if (reply_requested == 1) {
        try sendStandbyStatus(self, end);
        std.debug.print("REPLY REQUESTED!\n", .{});
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
