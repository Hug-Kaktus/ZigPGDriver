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
const sendStatement = extended_query.sendStatement;
const flushPipeline = extended_query.flushPipeline;
const readPipeline = extended_query.readPipeline;
const consume = extended_query.consume;
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
    const start = std.Io.Clock.awake.now(io);
    // ================== BIG INSERT ==================
    // var sql = try std.ArrayList(u8).initCapacity(conn.allocator, 2 * 1024 * 1024);
    // defer sql.deinit(conn.allocator);
    // try sql.print(conn.allocator, "INSERT INTO employee (first_name, last_name) VALUES\n", .{});
    // const end = 1000 * 100 + 1;
    // for (1..end) |i| {
    //     if (i != end - 1) {
    //         @branchHint(.likely);
    //         try sql.print(conn.allocator, "('Test{d}', 'Test{d}'),\n", .{ i, i });
    //     } else {
    //         try sql.print(conn.allocator, "('Test{d}', 'Test{d}');", .{ i, i });
    //     }
    // }
    // var query_result = try queryUntyped(conn, sql.items);
    // defer query_result.deinit(conn.allocator);

    // ================== BIG SELECT ==================
    // var query_result = try queryUntyped(conn, "SELECT * FROM employee LIMIT 100000");
    // defer query_result.deinit(conn.allocator);

    // ================== COMPLEX QUERY USING SIMPLE QUERY PROTOCOL ==================
    // ================== COMPLEX QUERY USING EXTENDED QUERY PROTOCOL ==================
    // ================== PIPELINE ==================
    // var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    // try param_types.append(conn.allocator, 0);
    // const stmt = try prepare(conn, "test_statement", "SELECT * FROM employee WHERE employee_id = $1", &param_types);
    // defer stmt.deinit(conn.allocator);
    // const end = 1000 * 100;
    // for (1..end + 1) |i| {
    //     var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
    //     defer values.deinit(conn.allocator);

    //     var buf: [20]u8 = undefined;
    //     const str = try std.fmt.bufPrint(&buf, "{d}", .{i});

    //     try values.append(conn.allocator, ParameterValue{
    //         .length = @intCast(str.len),
    //         .value = str,
    //     });

    //     try sendStatement(conn, stmt, values);
    // }
    // try flushPipeline(conn);
    // try readPipeline(conn);
    // ================== COMPARISON TO PIPELINE ==================
    // const end = 100000;
    // var sql = try std.ArrayList(u8).initCapacity(conn.allocator, 64);
    // defer sql.deinit(conn.allocator);
    // for (1..end) |i| {
    //     try sql.print(conn.allocator, "SELECT * FROM employee WHERE employee_id = {d}", .{i});
    //     defer sql.clearRetainingCapacity();
    //     var query_result = try queryUntyped(conn, sql.items);
    //     defer query_result.deinit(conn.allocator);
    // }

    // ================== COPY OUT ==================
    // var file = try std.Io.Dir.cwd().createFile(io, "out.csv", .{});
    // defer file.close(io);
    // var wbuf: [4096]u8 = undefined;
    // var writer = file.writer(io, &wbuf);
    // try copyToWriter(conn, "employee", &writer);

    // ================== COPY IN ==================
    // var file = try std.Io.Dir.cwd().openFile(io, "out.csv", .{});
    // defer file.close(io);
    // var rbuf: [4096]u8 = undefined;
    // var reader = file.reader(io, &rbuf);
    // try copyFromReader(conn, "employee", &reader);

    const duration = start.untilNow(io, .awake);
    std.debug.print("elapsed ms = {}\n", .{duration.toMilliseconds()});
}
