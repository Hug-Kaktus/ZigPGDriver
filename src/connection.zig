const std = @import("std");
const helpers = @import("helpers.zig");
const buildMessage = helpers.buildMessage;
const startup = @import("startup.zig").startup;

pub const Connection = struct {
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    rbuf: [4096]u8,
    wbuf: [4096]u8,
    reader: std.net.Stream.Reader,
    writer: std.net.Stream.Writer,

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
