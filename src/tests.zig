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
const dropReplicationSlot = replication.dropReplicationSlot;
const PluginOption = replication.PluginOption;
const createPublication = replication.createPublication;
const dropPublication = replication.dropPublication;

const copy = @import("copy.zig");
const copyToWriter = copy.copyToWriter;
const copyFromReader = copy.copyFromReader;

test "simple query untyped" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var result = try queryUntyped(conn, "SELECT employee_id, first_name, last_name FROM employee");
    defer result.deinit(conn.allocator);

    for (result.rows.items) |row| {
        const id = try row.getAs(i32, result.fields.items, "employee_id");
        const first_name = try row.getAs([]const u8, result.fields.items, "first_name");
        const last_name = try row.getAs([]const u8, result.fields.items, "last_name");

        std.debug.print("{} {s} {s}\n", .{ id, first_name, last_name });
    }
}

test "simple query typed" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }

    const Employee = struct {
        employee_id: i32,
        first_name: []const u8,
        last_name: []const u8,
    };
    var employees_result = try queryTyped(conn, Employee, "SELECT employee_id, first_name, last_name FROM employee");
    defer employees_result.deinit(conn.allocator);

    for (employees_result.rows.items) |e| {
        std.debug.print("{d} {s} {s}\n", .{ e.employee_id, e.first_name, e.last_name });
    }
}

test "create table" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn,
        \\CREATE TABLE employee (
        \\employee_id SERIAL PRIMARY KEY,
        \\first_name VARCHAR(50) NOT NULL,
        \\last_name VARCHAR(50) NOT NULL
        \\);
    );
    query_result.deinit(conn.allocator);
}

test "salary" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn,
        \\ CREATE TABLE salary (
        \\ salary_id SERIAL PRIMARY KEY,
        \\ employee_id INT REFERENCES employee(employee_id),
        \\ amount INT NOT NULL
        \\ );
    );
    defer query_result.deinit(conn.allocator);
}

test "join" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn,
        \\ SELECT 
        \\     e.first_name, 
        \\     e.last_name, 
        \\     s.amount AS annual_salary
        \\ FROM employee e
        \\ JOIN salary s ON e.employee_id = s.employee_id;
    );
    defer query_result.deinit(conn.allocator);
    for (query_result.rows.items) |row| {
        const first_name = try row.getAs([]const u8, query_result.fields.items, "first_name");
        const last_name = try row.getAs([]const u8, query_result.fields.items, "last_name");
        const annual_salary = try row.getAs(i32, query_result.fields.items, "amount");

        std.debug.print("{s} {s}, {d}\n", .{ first_name, last_name, annual_salary });
    }
}

test "drop tables" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn, "DROP TABLE IF EXISTS employee;");
    var query_result2 = try queryUntyped(conn, "DROP TABLE IF EXISTS salary;");
    defer query_result.deinit(conn.allocator);
    defer query_result2.deinit(conn.allocator);
}

test "insert" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn,
        \\INSERT INTO employee (first_name, last_name)
        \\VALUES
        \\('Oleksandr', 'Kovalskyi'),
        \\('Yana', 'Kikh'),
        \\('Kyrylo', 'Buherya'),
        \\('Viktoriia', 'Polyakova'),
        \\('Artem', 'Bondar');
    );
    defer query_result.deinit(conn.allocator);
}

test "salary insert" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn,
        \\INSERT INTO salary (employee_id, amount) VALUES
        \\(1, 65000),
        \\(2, 78500),
        \\(3, 52000);
    );
    defer query_result.deinit(conn.allocator);
}

test "remove all entries" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var query_result = try queryUntyped(conn, "DELETE FROM employee");
    defer query_result.deinit(conn.allocator);
}

test "give test data" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "::1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
}

test "extended query untyped" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }

    var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    try param_types.append(conn.allocator, 0);

    var prepared_statement = try prepare(conn, "test", "SELECT employee_id, first_name, last_name FROM employee WHERE employee_id = $1", &param_types);
    defer prepared_statement.deinit(conn.allocator);
    var parameter_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    defer parameter_format_codes.deinit(conn.allocator);
    try parameter_format_codes.append(conn.allocator, 0);

    var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
    defer values.deinit(conn.allocator);
    try values.append(conn.allocator, ParameterValue{ .length = 1, .value = "1" });

    var result_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    defer result_format_codes.deinit(conn.allocator);
    try result_format_codes.append(conn.allocator, 0);

    const binded_prepared_statement = try bindPreparedStatement(conn, "test_portal_name", prepared_statement, parameter_format_codes, values, result_format_codes);
    defer conn.allocator.destroy(binded_prepared_statement);

    var query_result = try executeQuery(conn, binded_prepared_statement, 0);
    defer query_result.deinit(conn.allocator);
    for (query_result.rows.items) |row| {
        const id = try row.getAs(i32, query_result.fields.items, "employee_id");
        const first_name = try row.getAs([]const u8, query_result.fields.items, "first_name");
        std.debug.print("{d} {s}\n", .{ id, first_name });
    }
}
test "extended query typed" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }

    const Employee = struct {
        employee_id: i32,
        first_name: []const u8,
        last_name: []const u8,
    };
    var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    try param_types.append(conn.allocator, 0);

    var prepared_statement = try prepare(conn, "test", "SELECT employee_id, first_name, last_name FROM employee WHERE employee_id = $1", &param_types);
    defer prepared_statement.deinit(conn.allocator);
    var parameter_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    defer parameter_format_codes.deinit(conn.allocator);
    try parameter_format_codes.append(conn.allocator, 0);

    var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
    defer values.deinit(conn.allocator);
    try values.append(conn.allocator, ParameterValue{ .length = 1, .value = "1" });

    var result_format_codes = try std.ArrayList(i16).initCapacity(conn.allocator, 1);
    defer result_format_codes.deinit(conn.allocator);
    try result_format_codes.append(conn.allocator, 0);

    const binded_prepared_statement = try bindPreparedStatement(conn, "test_portal_name", prepared_statement, parameter_format_codes, values, result_format_codes);
    defer conn.allocator.destroy(binded_prepared_statement);

    var employees_result = try executeQueryTyped(conn, Employee, binded_prepared_statement, 0);
    defer employees_result.deinit(conn.allocator);
    for (employees_result.rows.items) |e| {
        std.debug.print("{d} {s} {s}\n", .{ e.employee_id, e.first_name, e.last_name });
    }
}

test "pipelining" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var param_types = try std.ArrayList(i32).initCapacity(conn.allocator, 4);
    try param_types.append(conn.allocator, 0);
    const stmt = try prepare(conn, "test_statement", "SELECT * FROM employee WHERE employee_id = $1", &param_types);
    defer stmt.deinit(conn.allocator);
    for (1..6) |i| {
        var values = try std.ArrayList(ParameterValue).initCapacity(conn.allocator, 1);
        defer values.deinit(conn.allocator);

        var buf: [20]u8 = undefined;
        const str = try std.fmt.bufPrint(&buf, "{d}", .{i});

        try values.append(conn.allocator, ParameterValue{
            .length = @intCast(str.len),
            .value = str,
        });

        try sendStatement(conn, stmt, values);
    }
    try flushPipeline(conn);
    try readPipeline(conn);
    for (1..6) |_| {
        const q = try consume(conn);
        defer q.deinit(conn.allocator);
        for (q.rows.items) |row| {
            const id = try row.getAs(i32, q.prepared_statement.fields.items, "employee_id");
            const first_name = try row.getAs([]const u8, q.prepared_statement.fields.items, "first_name");
            const last_name = try row.getAs([]const u8, q.prepared_statement.fields.items, "last_name");

            std.debug.print("{} {s} {s}\n", .{ id, first_name, last_name });
        }
    }
}

test "copy out" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, "out.csv", .{});
    defer file.close(std.testing.io);

    var buf: [4096]u8 = undefined;

    var writer = file.writer(std.testing.io, &buf);
    try copyToWriter(conn, "employee", &writer);
}

test "copy in" {
    var conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "false",
    );
    defer {
        conn.close();
        std.testing.allocator.destroy(conn);
    }

    var file = try std.Io.Dir.cwd().openFile(std.testing.io, "data.csv", .{});
    defer file.close(std.testing.io);
    var buf: [4096]u8 = undefined;

    var reader = file.reader(std.testing.io, &buf);
    try copyFromReader(conn, "employee", &reader);
}

test "create logical replication slot" {
    var replication_conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        std.testing.allocator.destroy(replication_conn);
    }
    var create_resp = try createLogicalReplicationSlot(replication_conn, "test_logical_slot", "pgoutput");
    defer create_resp.deinit(replication_conn.allocator);
    // defer replication_conn.allocator.free(create_resp);
    create_resp.rows.items[0].show();
}

test "create publication" {
    var replication_conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        std.testing.allocator.destroy(replication_conn);
    }
    try createPublication(replication_conn, "test_publication");
}

test "drop publication" {
    var replication_conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        std.testing.allocator.destroy(replication_conn);
    }
    try dropPublication(replication_conn, "test_publication");
}

test "start logical replication" {
    var replication_conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        std.testing.allocator.destroy(replication_conn);
    }
    var options = try std.ArrayList(PluginOption).initCapacity(std.testing.allocator, 8);
    defer options.deinit(std.testing.allocator);
    try options.append(std.testing.allocator, .{ .name = "proto_version", .value = "4" });
    try options.append(std.testing.allocator, .{ .name = "publication_names", .value = "test_pub" });
    try startLogicalReplication(replication_conn, "test_logical_slot", "0/0", &options);
}

test "drop replication slot" {
    var replication_conn = try Connection.connect(
        std.testing.allocator,
        std.testing.io,
        "127.0.0.1",
        5432,
        "postgres",
        "1",
        "test",
        "database",
    );
    defer {
        replication_conn.close();
        std.testing.allocator.destroy(replication_conn);
    }
    try dropReplicationSlot(replication_conn, "test_logical_slot", false);
}
