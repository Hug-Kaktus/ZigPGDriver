const std = @import("std");

pub const FieldData = struct {
    table_object_id: i32,
    data_type_object_id: i32,
    type_modifier: i32,
    name: []const u8,
    data_type_size: i16,
    format_code: i16,
    column_attribute_number: i16,

    pub fn show(self: *const FieldData) void {
        std.debug.print("Field \"{s}\"\n", .{self.name});
        std.debug.print("    Data type: {s}\n", .{@tagName(oidToType(self.data_type_object_id))});
        std.debug.print("    Format: {s}\n", .{if (self.format_code == 0) "text" else "binary"});
    }
};

pub const Value = union(enum) {
    Int4: i32,
    Int8: i64,
    Float4: f32,
    Float8: f64,
    Bool: bool,
    Text: []const u8,
    Null,
};

pub const PgType = enum {
    Int4,
    Int8,
    Float4,
    Float8,
    Bool,
    Text,
    Unknown,
};

pub fn oidToType(oid: i32) PgType {
    return switch (oid) {
        23 => .Int4,
        20 => .Int8,
        700 => .Float4,
        701 => .Float8,
        16 => .Bool,
        25, 1043 => .Text,
        else => .Unknown,
    };
}

pub fn convertValue(comptime T: type, v: Value) !T {
    return switch (@typeInfo(T)) {
        .int => |ti| switch (v) {
            .Int4 => |x| {
                if (ti.bits == 32) {
                    return @as(T, x);
                } else return error.TypeMismatch;
            },
            .Int8 => |x| {
                if (ti.bits == 64) {
                    return @as(T, x);
                } else return error.TypeMismatch;
            },
            else => error.TypeMismatch,
        },
        .float => switch (v) {
            .Float4 => |x| @as(T, x),
            .Float8 => |x| @as(T, x),
            else => error.TypeMismatch,
        },
        .bool => switch (v) {
            .Bool => |b| b,
            else => error.TypeMismatch,
        },
        .pointer => switch (v) {
            .Text => |s| s,
            else => error.TypeMismatch,
        },
        .optional => |opt| {
            if (v == .Null) return null;
            return try convertValue(opt.child, v);
        },
        else => error.UnsupportedType,
    };
}

pub const Row = struct {
    values: []Value,

    pub fn getAs(
        self: Row,
        comptime T: type,
        fields: []FieldData,
        name: []const u8,
    ) !T {
        for (fields, 0..) |f, i| {
            if (std.mem.eql(u8, f.name, name)) {
                return try convertValue(T, self.values[i]);
            }
        }
        return error.ColumnNotFound;
    }
};

pub const QueryResult = struct {
    arena: std.heap.ArenaAllocator,
    fields: std.ArrayList(FieldData),
    rows: std.ArrayList(Row),

    pub fn deinit(self: *QueryResult) void {
        self.arena.deinit();
    }
};

pub const PreparedStatement = struct {
    name: []const u8,
    fields: std.ArrayList(FieldData),
    parameter_count: i32,
    parameters: std.ArrayList(i32),

    pub fn show(self: *const PreparedStatement) void {
        std.debug.print("Prepared statement \"{s}\"\n", .{self.name});
        std.debug.print("Fields:\n", .{});
        for (self.fields.items) |field| {
            field.show();
        }
        std.debug.print("Parameter count: {d}\n", .{self.parameter_count});
        std.debug.print("Parameters:\n", .{});
        for (self.parameters.items, 1..) |parameter, i| {
            std.debug.print("{d}. {s}\n", .{i, @tagName(oidToType(parameter))});
        }
    }
};

pub const BindedPreparedStatement = struct {
    prepared_statement: *const PreparedStatement,
    portal_name: []const u8,

    pub fn show(self: *const BindedPreparedStatement) void {
        self.prepared_statement.show();
        std.debug.print("Portal name: {s}\n", .{self.portal_name});
    }
};

const QueryState = enum {
    pending,
    done,
    failed,
    skipped,
};

pub const PendingQuery = struct {
    arena: std.heap.ArenaAllocator,
    prepared_statement: PreparedStatement,
    rows: std.ArrayList(Row),
    state: QueryState,
    error_message: ?[]u8,
};

pub const Data = struct {
    name: []const u8,
    setting: []const u8,
    source: []const u8,
    sourcefile: []const u8,

    pub fn show(self: *const Data) void {
        std.debug.print("name: {s}\n", .{self.name});
        std.debug.print("setting: {s}\n", .{self.setting});
        std.debug.print("source: {s}\n", .{self.source});
        std.debug.print("sourcefile: {s}\n", .{self.sourcefile});
    }
};
