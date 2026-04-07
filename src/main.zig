const std = @import("std");
const Connection = @import("connection.zig").Connection;
const query = @import("query.zig");
const queryWithFields = query.queryWithFields;
const queryTyped = query.queryTyped;
const extended_query = @import("extended_query.zig");
const parse = extended_query.parse;
const bind = extended_query.bind;

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

    // var params = try std.ArrayList(i32).initCapacity(allocator, 0);
    // defer params.deinit(allocator);
    // try parse(&conn, "test_query", "SELECT 1;", params);
    // try bind(&conn, "test_query", "SELECT 1;", , parameter_values: Aligned(ParameterValue), result_column_format_codes: Aligned(i16))

    // _ = try queryWithFields(&conn,
    // \\CREATE TABLE employee (
    // \\employee_id SERIAL PRIMARY KEY,
    // \\first_name VARCHAR(50) NOT NULL,
    // \\last_name VARCHAR(50) NOT NULL
    // \\);
    // );

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

    const Employee = struct {
        employee_id: i32,
        first_name: []const u8,
        last_name: []const u8,
    };

    const employees = try queryTyped(&conn, Employee, "SELECT employee_id, first_name, last_name FROM employee");

    for (employees.items) |e| {
        std.debug.print("{d} {s} {s}\n", .{e.employee_id, e.first_name, e.last_name});
    }
}
