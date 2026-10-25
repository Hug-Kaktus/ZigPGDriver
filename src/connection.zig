const std = @import("std");
const helpers = @import("helpers.zig");
const startup = @import("startup.zig").startup;
const types = @import("types.zig");
const RingQueue = types.RingQueue;
const PendingQuery = types.PendingQuery;
const ScramState = types.ScramState;

const BackendKeyData = struct {
    in_hot_standby: []const u8,
    integer_datetimes: []const u8,
    TimeZone: []const u8,
    IntervalStyle: []const u8,
    search_path: []const u8,
    is_superuser: []const u8,
    application_name: []const u8,
    default_transaction_read_only: []const u8,
    scram_iterations: []const u8,
    DateStyle: []const u8,
    standard_conforming_strings: []const u8,
    session_authorization: []const u8,
    client_encoding: []const u8,
    server_version: []const u8,
    server_encoding: []const u8,
};

pub const Connection = struct {
    id: usize,
    io: std.Io,
    stream: std.Io.net.Stream,
    allocator: std.mem.Allocator,
    rbuf: [4096]u8,
    wbuf: [4096]u8,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    pending: RingQueue(*PendingQuery),
    completed: RingQueue(*PendingQuery),
    user: []const u8,
    scram_state: ?ScramState,
    oauth_token: ?[]const u8,

    in_use: bool,

    process_id: i32,
    secret_key_len: i32,
    secret_key: [256]u8,
    backend_key_data: BackendKeyData,
    closed: bool,
    broken: bool,

    streaming: bool,
    parallel: bool,

    pub fn connect(
        id: usize,
        allocator: std.mem.Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
        replication: []const u8,
    ) !*Connection {
        var addr = try std.Io.net.IpAddress.parse(host, port);
        var stream = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream });

        var connection = try allocator.create(Connection);
        connection.* = Connection{
            .id = id,
            .io = io,
            .stream = stream,
            .allocator = allocator,
            .rbuf = undefined,
            .wbuf = undefined,
            .reader = undefined,
            .writer = undefined,
            .pending = try RingQueue(*PendingQuery).init(
                allocator,
                8,
            ),
            .completed = try RingQueue(*PendingQuery).init(
                allocator,
                8,
            ),

            .user = user,
            .scram_state = null,
            .oauth_token = null,

            .in_use = false,

            .process_id = undefined,
            .secret_key_len = undefined,
            .secret_key = undefined,
            .backend_key_data = undefined,

            .streaming = false,
            .parallel = false,
        };
        connection.reader = stream.reader(io, &connection.rbuf);
        connection.writer = stream.writer(io, &connection.wbuf);
        errdefer connection.close();

        try startup(connection, user, password, database, replication);
        return connection;
    }

    /// Begins the transaction
    pub fn begin(self: *Connection) !void {}

    /// Commits pending transactions to the database
    pub fn commit(self: *Connection) !void {}

    /// Roll back to the start of any pending transaction
    pub fn rollback(self: *Connection) !void {}

    pub const TransactionStatus = enum {
        not_started,
        active,
        commited,
        rolled_back_explicitly,
        rolled_back_with_error,
        failed,
    };

    pub const Transaction = struct {
        connection: *Connection,
        savepoint_name: []const u8,
        status: TransactionStatus,
    };

    pub fn transaction(
        self: *Connection,
        savepoint_name: []const u8,
        force_rollback: bool,
    ) !Transaction {}

    pub fn isAlive(self: *Connection) !bool {
        try self.writer.interface.writeByte('Q');
        try self.writer.interface.writeInt(i32, 4 + 9 + 1, .big);
        try self.writer.interface.writeAll("SELECT 1;");
        try self.writer.interface.writeByte(0);
        try self.writer.interface.flush();

        var r = &self.reader;
        while (true) {
            const msg_type = try r.interface.takeByte();
            const payload_len: usize = @intCast(try r.interface.takeInt(i32, .big) - 4);
            switch (msg_type) {
                'T' => {
                    _ = try r.interface.take(payload_len);
                },
                'D' => {
                    _ = try r.interface.take(payload_len);
                },
                'C' => {
                    _ = (try r.interface.takeDelimiter(0)).?;
                    return true;
                },
                'I' => {
                    return true;
                },
                'Z' => {
                    _ = try r.interface.takeByte();
                    return true;
                },
                'E' => {
                    _ = try r.interface.take(payload_len);
                    return error.ServerError;
                },
                'N' => {
                    _ = try r.interface.take(payload_len);
                },
                else => {
                    std.debug.print("Unsupported message type {c}\n", .{msg_type});
                    return error.UnknownMessageType;
                },
            }
        }
    }

    pub fn close(self: *Connection) void {
        while (self.pending.pop()) |p| {
            p.deinit(self.allocator);
        }
        while (self.completed.pop()) |p| {
            p.deinit(self.allocator);
        }
        self.pending.deinit();
        self.completed.deinit();
        self.stream.close(self.io);
        if (self.scram_state) |*value| {
            value.*.deinit(self.allocator);
        }
    }
};

const Lease = struct {
    pool: *Pool,
    connection: *Connection,

    pub fn release(self: *Lease) void {
        self.pool.release(self.connection) catch {
            unreachable;
        };
    }
};
pub const Pool = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,

    mutex: std.Io.Mutex = std.Io.Mutex.init,
    condition: std.Io.Condition = std.Io.Condition.init,
    connections: std.ArrayList(*Connection) = std.ArrayList(*Connection).empty,
    min_connections: u16,
    max_connections: ?u16,
    check: ?*const fn (*Connection) void,
    reset: ?*const fn (*Connection) void,
    timeout: u32,
    max_waiting_requests: u32,
    max_connection_lifetime: u32,
    max_idle_connection_time: u32,
    reconnect_timeout: u32,
    reconnect_failed: ?*const fn (*Pool) void,
    next_id: usize,
    num_workers: u16,

    pub const Config = struct {
        port: u16 = 5432,
        min_connections: u16 = 4,
        max_connections: ?u16 = null,
        timeout_ms: u32 = 5000,
        enable_logging: bool = false,
        name: ?[]const u8 = null,
    };

    pub fn init(
        io: std.Io,
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
        max_connections: usize,
    ) !Pool {
        return .{
            .io = io,
            .allocator = allocator,
            .host = host,
            .port = port,
            .user = user,
            .password = password,
            .database = database,
            .max_connections = max_connections,
            .next_id = 1,
        };
    }

    pub fn deinit(self: *Pool) void {
        self.mutex.lock(self.io) catch {
            unreachable;
        };
        defer self.mutex.unlock(self.io);
        for (self.connections.items) |c| {
            c.close();
        }
        for (self.connections.items) |connection| {
            self.allocator.destroy(connection);
        }
        self.connections.deinit(self.allocator);
    }

    pub fn acquire(self: *Pool) !Lease {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        while (true) {
            for (self.connections.items) |connection| {
                if (!connection.in_use and try connection.isAlive()) {
                    connection.in_use = true;
                    return .{ .pool = self, .connection = connection };
                }
            }
            if (self.connections.items.len < self.max_connections) {
                const conn = try Connection.connect(
                    self.next_id,
                    self.allocator,
                    self.io,
                    self.host,
                    self.port,
                    self.user,
                    self.password,
                    self.database,
                    "false",
                );
                try self.connections.append(self.allocator, conn);
                return .{ .pool = self, .connection = self.connections.items[self.connections.items.len - 1] };
            }
            try self.condition.wait(self.io, &self.mutex);
        }
    }

    pub fn release(self: *Pool, connection: *Connection) std.Io.Cancelable!void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        for (self.connections.items) |c| {
            if (c == connection) {
                c.in_use = false;
                self.condition.signal(self.io);
                return;
            }
        }
    }
};
