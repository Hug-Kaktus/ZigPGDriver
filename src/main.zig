const std = @import("std");
const Connection = @import("connection.zig").Connection;

const query = @import("query.zig");
const queryWithFields = query.queryWithFields;
const queryTyped = query.queryTyped;

const extended_query = @import("extended_query.zig");
const prepare = extended_query.prepare;
const bindPreparedStatement = extended_query.bindPreparedStatement;
const executeQuery = extended_query.executeQuery;
const executeQueryTyped = extended_query.executeQueryTyped;
const ParameterValue = extended_query.ParameterValue;

const replication = @import("replication.zig");
const identifySystem = replication.identifySystem;
const showParam = replication.showParam;
const timelineHistory = replication.timelineHistory;
const createPhysicalReplicationSlot = replication.createPhysicalReplicationSlot;
const createLogicalReplicationSlot = replication.createLogicalReplicationSlot;
const alterReplicationSlot = replication.alterReplicationSlot;
const readReplicationSlot = replication.readReplicationSlot;
const startLogicalReplication = replication.startLogicalReplication;
const PluginOption = replication.PluginOption;
const createPublication = replication.createPublication;
const copy = @import("copy.zig");
const copyToWriter = copy.copyToWriter;
const copyFromReader = copy.copyFromReader;

const Data = @import("types.zig").Data;

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();

    var conn = try Connection.connect(
        allocator,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer conn.close();

    // ===== Replication protocol test =====
    var replication_conn = try Connection.connect(
        allocator,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer replication_conn.close();
    // var options = try std.ArrayList(PluginOption).initCapacity(allocator, 8);
    // try options.append(allocator, .{.name = "proto_version", .value = "4"});
    // try options.append(allocator, .{.name = "publication_names", .value = "test_pub"});
    // try startLogicalReplication(&replication_conn, "test_logical_slot", "0/0", options);
    var file = try std.fs.cwd().openFile("data.csv", .{});
    defer file.close();
    var buf: [4096]u8 = undefined;

    var reader = file.reader(&buf);
    try copyFromReader(&conn, "employee", &reader);
}
