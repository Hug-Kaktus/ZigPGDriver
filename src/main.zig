const std = @import("std");
const Connection = @import("connection.zig").Connection;
const query = @import("query.zig");
const queryWithFields = query.queryWithFields;
const queryTyped = query.queryTyped;
const extended_query = @import("extended_query.zig");
const queryExt = extended_query.queryExt;
const prepare = extended_query.prepare;
const bindPreparedStatement = extended_query.bindPreparedStatement;
const executeQuery = extended_query.executeQuery;
const executeQueryTyped = extended_query.executeQueryTyped;
const ParameterValue = extended_query.ParameterValue;

pub fn main() !void {
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

    // const query_result = try executeQuery(&conn, &binded_prepared_statement, 0);
    // for (query_result.rows.items) |row| {
    //     const id = try row.getAs(i32, query_result.fields.items, "employee_id");
    //     const first_name = try row.getAs([]const u8, query_result.fields.items, "first_name");
    //     std.debug.print("{d} {s}\n", .{id, first_name});
    // }
    //

    // _ = try queryWithFields(&conn,
    // \\CREATE TABLE employee (
    // \\employee_id SERIAL PRIMARY KEY,
    // \\first_name VARCHAR(50) NOT NULL,
    // \\last_name VARCHAR(50) NOT NULL
    // \\);
    // );
    // _ = try queryWithFields(&conn, "DROP TABLE employee;");

    // _ = try queryWithFields(&conn,
    // \\INSERT INTO employee (first_name, last_name)
    // \\VALUES
    // \\('Oleksandr', 'Kovalskyi'),
    // \\('Yana', 'Kikh'),
    // \\('Kyrylo', 'Buherya'),
    // \\('Viktoriia', 'Polyakova'),
    // \\('Artem', 'Bondar');
    // );

    // const result = try queryWithFields(&conn, "SELECT employee_id, first_name, last_name FROM employee");

    // for (result.rows.items) |row| {
    //     const id = try row.getAs(i32, result.fields.items, "employee_id");
    //     const first_name = try row.getAs([]const u8, result.fields.items, "first_name");
    //     const last_name = try row.getAs([]const u8, result.fields.items, "last_name");

    //     std.debug.print("{} {s} {s}\n", .{id, first_name, last_name});
    // }

    // const employees = try queryTyped(&conn, Employee, "SELECT employee_id, first_name, last_name FROM employee");

    // for (employees.items) |e| {
    //     std.debug.print("{d} {s} {s}\n", .{e.employee_id, e.first_name, e.last_name});
    // }

    // const result = try queryExt(
    //     &conn,
    //     "SELECT employee_id, first_name FROM employee WHERE employee_id = $1",
    //     @constCast(&[_]ParameterValue{
    //         .{ .length = 1, .value = "1" },
    //     }),
    // );
    // for (result.rows.items) |row| {
    //     const id = try row.getAs(i32, result.fields.items, "employee_id");
    //     const first_name = try row.getAs([]const u8, result.fields.items, "first_name");

    //     std.debug.print("{} {s}\n", .{id, first_name});
    // }
}
