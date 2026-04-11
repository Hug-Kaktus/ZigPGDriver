const std = @import("std");
const Connection = @import("connection.zig").Connection;
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;

pub fn startReplicationConnection(self: *Connection, replication_mode: []const u8) !void {
    try self.writer.interface.writeInt(i32, 8 + 4 + replication_mode.len + 1, .big);
    try self.writer.interface.writeInt(i32, 196610, .big);

    try self.writer.interface.writeAll("replication");
    try self.writer.interface.writeByte(0);
    try self.writer.interface.writeAll(replication_mode);
    try self.writer.interface.writeByte(0);

    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn startPhysicalReplication(
    self: *Connection,
    start_lsn: []const u8,
) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    try buf.appendSlice("START_REPLICATION ");
    try buf.appendSlice(start_lsn);

    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, @intCast(4 + buf.items.len + 1), .big);
    try self.writer.interface.writeAll(buf.items);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn startLogicalReplication(
    self: *Connection,
    slot: []const u8,
    start_lsn: []const u8,
) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    try buf.appendSlice("START_REPLICATION SLOT ");
    try buf.appendSlice(slot);
    try buf.appendSlice(" LOGICAL ");
    try buf.appendSlice(start_lsn);
    try buf.appendSlice(" (proto_version '1', publication_names 'pub1')");

    try self.writer.interface.writeByte('Q');
    try self.writer.interface.writeInt(i32, @intCast(4 + buf.items.len + 1), .big);
    try self.writer.interface.writeAll(buf.items);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn readReplicationStream(self: *Connection) !void {
    var reader = self.reader.interface();

    while (true) {
        const msg_type = try reader.takeByte();
        const len = try reader.takeInt(i32, .big);

        switch (msg_type) {
            'W' => {
                std.debug.print("CopyBothResponse\n", .{});
                try reader.skipBytes(len - 4, .{});
            },
            'd' => {
                try self.handleCopyData(len - 4);
            },
            'c' => {
                std.debug.print("CopyDone\n", .{});
                return;
            },
            'E' => {
                const err = try buildMessage(self.allocator, reader);
                std.debug.print("{s}\n", .{err.items});
                return error.ServerError;
            },
            else => {
                std.debug.print("Unknown replication msg: {c}\n", .{msg_type});
                try reader.skipBytes(len - 4, .{});
            },
        }
    }
}

pub fn handleCopyData(self: *Connection, size: i32) !void {
    var reader = self.reader.interface();

    const subtype = try reader.takeByte();

    switch (subtype) {
        'w' => {
            try self.handleXLogData(size - 1);
        },
        'k' => {
            try self.handleKeepalive(size - 1);
        },
        else => {
            std.debug.print("Unknown CopyData subtype: {c}\n", .{subtype});
            try reader.skipBytes(size - 1, .{});
        },
    }
}

pub fn handleXLogData(self: *Connection, size: i32) !void {
    var reader = self.reader.interface();

    const start_lsn = try reader.takeInt(u64, .big);
    const end_lsn = try reader.takeInt(u64, .big);
    // const timestamp = try reader.takeInt(i64, .big);

    const payload_size = size - 8 - 8 - 8;

    var buf = try self.allocator.alloc(u8, @intCast(payload_size));
    defer self.allocator.free(buf);

    try reader.readNoEof(&buf);

    std.debug.print(
        "WAL: start={} end={} size={}\n",
        .{ start_lsn, end_lsn, payload_size }
    );
}

pub fn handleKeepalive(self: *Connection) !void {
    var reader = self.reader.interface();

    const end_lsn = try reader.takeInt(u64, .big);
    // const timestamp = try reader.takeInt(i64, .big);
    const reply_requested = try reader.takeByte();

    std.debug.print(
        "Keepalive: lsn={} reply={}\n",
        .{ end_lsn, reply_requested }
    );

    if (reply_requested == 1) {
        try self.sendStandbyStatus(end_lsn);
    }
}

pub fn sendStandbyStatus(self: *Connection, lsn: u64) !void {
    try self.writer.interface.writeByte('d');

    const len: i32 = 4 + 1 + 8 + 8 + 8 + 8 + 1;
    try self.writer.interface.writeInt(i32, len, .big);

    try self.writer.interface.writeByte('r');

    try self.writer.interface.writeInt(u64, lsn, .big);
    try self.writer.interface.writeInt(u64, lsn, .big);
    try self.writer.interface.writeInt(u64, lsn, .big);

    try self.writer.interface.writeInt(i64, 0, .big);
    try self.writer.interface.writeByte(0);

    try self.writer.interface.flush();
}
