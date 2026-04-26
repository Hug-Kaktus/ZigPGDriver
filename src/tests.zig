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
const dropReplicationSlot = replication.dropReplicationSlot;
const PluginOption = replication.PluginOption;
const createPublication = replication.createPublication;
const dropPublication = replication.dropPublication;


test "simple query untyped" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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
    const result = try queryWithFields(&conn, "SELECT employee_id, first_name, last_name FROM employee");

    for (result.rows.items) |row| {
        const id = try row.getAs(i32, result.fields.items, "employee_id");
        const first_name = try row.getAs([]const u8, result.fields.items, "first_name");
        const last_name = try row.getAs([]const u8, result.fields.items, "last_name");

        std.debug.print("{} {s} {s}\n", .{id, first_name, last_name});
    }

}
test "simple query typed" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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

    const Employee = struct {
        employee_id: i32,
        first_name: []const u8,
        last_name: []const u8,
    };
    const employees = try queryTyped(&conn, Employee, "SELECT employee_id, first_name, last_name FROM employee");

    for (employees.items) |e| {
        std.debug.print("{d} {s} {s}\n", .{e.employee_id, e.first_name, e.last_name});
    }
    try std.testing.expect(true);
}

test "create table" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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
    _ = try queryWithFields(&conn,
    \\CREATE TABLE employee (
    \\employee_id SERIAL PRIMARY KEY,
    \\first_name VARCHAR(50) NOT NULL,
    \\last_name VARCHAR(50) NOT NULL
    \\);
    );
}

test "drop table" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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
    _ = try queryWithFields(&conn, "DROP TABLE employee;");
}

test "insert" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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
    _ = try queryWithFields(&conn,
    \\INSERT INTO employee (first_name, last_name)
    \\VALUES
    \\('Oleksandr', 'Kovalskyi'),
    \\('Yana', 'Kikh'),
    \\('Kyrylo', 'Buherya'),
    \\('Viktoriia', 'Polyakova'),
    \\('Artem', 'Bondar');
    );
}

test "extended query untyped" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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

    var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    try param_types.append(conn.allocator, 0);

    const prepared_statement = try prepare(
        &conn,
        "test", "SELECT employee_id, first_name, last_name FROM employee WHERE employee_id = $1",
        &param_types
    );
    var parameter_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    try parameter_format_codes.append(conn.allocator, 0);

    var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
    try values.append(conn.allocator, ParameterValue{.length = 1, .value = "1"});

    var result_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    try result_format_codes.append(conn.allocator, 0);

    const binded_prepared_statement = try bindPreparedStatement(&conn,
        "test_portal_name",
        &prepared_statement,
        parameter_format_codes,
        values,
        result_format_codes
    );

    const query_result = try executeQuery(&conn, &binded_prepared_statement, 0);
    for (query_result.rows.items) |row| {
        const id = try row.getAs(i32, query_result.fields.items, "employee_id");
        const first_name = try row.getAs([]const u8, query_result.fields.items, "first_name");
        std.debug.print("{d} {s}\n", .{id, first_name});
    }

}
test "extended query typed" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
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

    const Employee = struct {
        employee_id: i32,
        first_name: []const u8,
        last_name: []const u8,
    };
    var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    try param_types.append(conn.allocator, 0);

    const prepared_statement = try prepare(
        &conn,
        "test", "SELECT employee_id, first_name, last_name FROM employee WHERE employee_id = $1",
        &param_types
    );
    var parameter_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    try parameter_format_codes.append(conn.allocator, 0);

    var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
    try values.append(conn.allocator, ParameterValue{.length = 1, .value = "1"});

    var result_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    try result_format_codes.append(conn.allocator, 0);

    const binded_prepared_statement = try bindPreparedStatement(&conn,
        "test_portal_name",
        &prepared_statement,
        parameter_format_codes,
        values,
        result_format_codes
    );

    const employees = try executeQueryTyped(&conn, Employee, &binded_prepared_statement, 0);
    for (employees.items) |e| {
        std.debug.print("{d} {s} {s}\n", .{e.employee_id, e.first_name, e.last_name});
    }
}

test "create logical replication slot" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

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
    const create_resp = try createLogicalReplicationSlot(&replication_conn, "test_logical_slot", "pgoutput");
    create_resp.show();
}

test "create publication" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

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
    try createPublication(&replication_conn, "test_publication");
}

test "drop publication" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

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
    try dropPublication(&replication_conn, "test_publication");
}

test "start logical replication" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

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
    var options = try std.ArrayList(PluginOption).initCapacity(allocator, 8);
    try options.append(allocator, .{.name = "proto_version", .value = "4"});
    try options.append(allocator, .{.name = "publication_names", .value = "test_pub"});
    try startLogicalReplication(&replication_conn, "test_logical_slot", "0/0", options);
}

test "drop replication slot" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

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
    dropReplicationSlot(&replication_conn, "test_logical_slot", false);
}
