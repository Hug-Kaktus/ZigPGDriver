const std = @import("std");
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const startup = @import("startup.zig").startup;
const types = @import("types.zig");
const PendingQuery = types.PendingQuery;

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
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    rbuf: [4096]u8,
    wbuf: [4096]u8,
    reader: std.net.Stream.Reader,
    writer: std.net.Stream.Writer,
    pending: std.ArrayList(PendingQuery),

    process_id: i32,
    secret_key_len: i32,
    secret_key: [256]u8,
    backend_key_data: BackendKeyData,

    pub fn connect(
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
        replication: []const u8,
    ) !Connection {
        const address = try std.net.Address.parseIp(host, port);
        var stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        var connection = Connection{
            .stream = stream,
            .allocator = allocator,
            .rbuf = undefined,
            .wbuf = undefined,
            .reader = undefined,
            .writer = undefined,
            .pending = try std.ArrayList(PendingQuery).initCapacity(allocator, 8),
            .process_id = undefined,
            .secret_key_len = undefined,
            .secret_key = undefined,
            .backend_key_data = undefined,
        };
        connection.reader = stream.reader(&connection.rbuf);
        connection.writer = stream.writer(&connection.wbuf);

        try startup(&connection, user, password, database, replication);
        return connection;
    }

    pub fn close(self: *Connection) void {
        self.stream.close();
    }
};
