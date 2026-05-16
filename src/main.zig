const std = @import("std");
const Connection = @import("connection.zig").Connection;

const query = @import("query.zig");
const queryUntyped = query.queryUntyped;
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

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var conn = try Connection.connect(
        gpa,
        io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        gpa.destroy(conn);
    }
    // const Employee = struct {
    //     employee_id: i32,
    //     first_name: []const u8,
    //     last_name: []const u8,
    // };
    // var employees_result = try queryTyped(conn, Employee, "SELECT employee_id, first_name, last_name FROM employee");
    // defer employees_result.deinit();

    // for (employees_result.rows.items) |e| {
    //     std.debug.print("{d} {s} {s}\n", .{ e.employee_id, e.first_name, e.last_name });
    // }

    // var options = try std.ArrayList(PluginOption).initCapacity(allocator, 8);
    // try options.append(allocator, .{.name = "proto_version", .value = "4"});
    // try options.append(allocator, .{.name = "publication_names", .value = "test_pub"});
    // try startLogicalReplication(&replication_conn, "test_logical_slot", "0/0", options);
    var replication_conn = try Connection.connect(
        gpa,
        io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        replication_conn.allocator.destroy(replication_conn);
    }
    var options = try std.ArrayList(PluginOption).initCapacity(replication_conn.allocator, 8);
    defer options.deinit(replication_conn.allocator);
    try options.append(replication_conn.allocator, .{ .name = "proto_version", .value = "4" });
    try options.append(replication_conn.allocator, .{ .name = "publication_names", .value = "test_pub" });
    try startLogicalReplication(replication_conn, "test_logical_slot", "0/0", &options);
}
