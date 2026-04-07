const std = @import("std");
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const startup = @import("startup.zig").startup;

const BackendKeyData = struct {
    in_hot_standby: []const u8, // 14
    integer_datetimes: []const u8, // 17
    TimeZone: []const u8, // 8
    IntervalStyle: []const u8, // 13
    search_path: []const u8, // 11
    is_superuser: []const u8, // 12
    application_name: []const u8, // 16
    default_transaction_read_only: []const u8, // 29
    scram_iterations: []const u8, // 16
    DateStyle: []const u8, // 9
    standard_conforming_strings: []const u8, // 27
    session_authorization: []const u8, // 21
    client_encoding: []const u8, // 15
    server_version: []const u8, // 14
    server_encoding: []const u8, // 15
};

pub const Connection = struct {
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    rbuf: [4096]u8,
    wbuf: [4096]u8,
    reader: std.net.Stream.Reader,
    writer: std.net.Stream.Writer,

    process_id: i32,
    secret_key: []const u8,
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
            .process_id = undefined,
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
