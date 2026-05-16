const std = @import("std");

pub const ScramState = struct {
    client_nonce: []const u8,
    server_nonce: []const u8,
    salt: []const u8,
    iterations: u32,
    client_first_bare: []const u8,

    auth_message: []u8,
    salted_password: [32]u8,

    pub fn show(self: *const ScramState) void {
        std.debug.print("===== ScramState =====\n", .{});
        std.debug.print("client_nonce: {s}\n", .{self.client_nonce});
        std.debug.print("server_nonce: {s}\n", .{self.server_nonce});
        std.debug.print("salt: {x}\n", .{self.salt});
        std.debug.print("iterations: {d}\n", .{self.iterations});
        std.debug.print("client_first_bare: {s}\n", .{self.client_first_bare});
        std.debug.print("auth_message: {s}\n", .{self.auth_message});
        std.debug.print("salted_password: {s}\n", .{self.salted_password});
    }

    pub fn deinit(self: *ScramState, allocator: std.mem.Allocator) void {
        // allocator.free(self.client_nonce);
        allocator.free(self.server_nonce);
        allocator.free(self.salt);
        allocator.free(self.client_first_bare);
        allocator.free(self.auth_message);
    }
};

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
    arena: *std.heap.ArenaAllocator,
    fields: std.ArrayList(FieldData),
    rows: std.ArrayList(Row),

    pub fn deinit(self: *QueryResult, allocator: std.mem.Allocator) void {
        self.arena.deinit();
        allocator.destroy(self.arena);
        // allocator.destroy(self);
    }
};

pub fn TypedQueryResult(comptime T: type) type {
    return struct {
        arena: *std.heap.ArenaAllocator,
        rows: std.ArrayList(T),

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.arena.deinit();
            allocator.destroy(self.arena);
        }
    };
}

pub const PreparedStatement = struct {
    arena: *std.heap.ArenaAllocator,
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
            std.debug.print("{d}. {s}\n", .{ i, @tagName(oidToType(parameter)) });
        }
    }

    pub fn deinit(self: *PreparedStatement, allocator: std.mem.Allocator) void {
        self.arena.deinit();
        allocator.destroy(self.arena);
        allocator.destroy(self);
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
    arena: *std.heap.ArenaAllocator,
    prepared_statement: *PreparedStatement,
    rows: std.ArrayList(Row),
    state: QueryState,
    error_message: ?[]u8,

    pub fn deinit(self: *PendingQuery, allocator: std.mem.Allocator) void {
        self.rows.deinit(self.arena.allocator());
        self.arena.deinit();
        allocator.destroy(self.arena);
        allocator.destroy(self);
    }
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

pub fn RingQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: []T,
        head: usize,
        tail: usize,
        count: usize,

        pub fn init(
            allocator: std.mem.Allocator,
            cap: usize,
        ) !Self {
            return .{
                .allocator = allocator,
                .items = try allocator.alloc(T, cap),
                .head = 0,
                .tail = 0,
                .count = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }

        pub fn len(self: *const Self) usize {
            return self.count;
        }

        pub fn capacity(self: *const Self) usize {
            return self.items.len;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn isFull(self: *const Self) bool {
            return self.count == self.items.len;
        }

        pub fn push(self: *Self, item: T) !void {
            if (self.isFull()) {
                try self.resize(self.items.len * 2);
            }

            self.items[self.tail] = item;
            self.tail = (self.tail + 1) % self.items.len;
            self.count += 1;
        }

        pub fn pop(self: *Self) ?T {
            if (self.isEmpty()) {
                return null;
            }

            const item = self.items[self.head];

            self.head = (self.head + 1) % self.items.len;
            self.count -= 1;

            return item;
        }

        pub fn peek(self: *Self) ?T {
            if (self.isEmpty()) {
                return null;
            }

            return self.items[self.head];
        }

        pub fn resize(self: *Self, new_capacity: usize) !void {
            const new_buffer = try self.allocator.alloc(T, new_capacity);

            for (0..self.count) |i| {
                new_buffer[i] =
                    self.items[(self.head + i) % self.items.len];
            }

            self.allocator.free(self.items);

            self.items = new_buffer;
            self.head = 0;
            self.tail = self.count;
        }
    };
}
