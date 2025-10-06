const std = @import("std");

pub const Connection = struct {
    stream: std.net.Stream,

    pub fn connect(
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
    ) !Connection {
        const address = try std.net.Address.parseIp(host, port);
        var stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        // --- Startup Message ---
        var buf = std.ArrayList(u8).init(allocator);
        defer buf.deinit();

        // Protocol version: 3.0
        try buf.writer().writeInt(u32, 196608, .big); // 3 << 16
        try writeCString(buf.writer(), "user");
        try writeCString(buf.writer(), user);
        try writeCString(buf.writer(), "database");
        try writeCString(buf.writer(), database);
        try writeCString(buf.writer(), "client_encoding");
        try writeCString(buf.writer(), "UTF8");
        try buf.writer().writeByte(0);

        const total_len = buf.items.len + 4;
        var start_msg = std.ArrayList(u8).init(allocator);
        defer start_msg.deinit();

        try start_msg.writer().writeInt(u32, @intCast(total_len), .big);
        try start_msg.appendSlice(buf.items);

        try stream.writer().writeAll(start_msg.items);

        // --- Read server response ---
        var reader = stream.reader();
        var tmp: [1024]u8 = undefined;
        const n = try reader.read(&tmp);
        if (n == 0) return error.ConnectionClosed;

        var i: usize = 0;
        while (i < n) {
            const msg_type = tmp[i];
            const len = std.mem.readInt(u32, tmp[i + 1 ..][0..4], .big);
            i += 5;

            switch (msg_type) {
                'R' => { // Authentication request
                    const auth_type = std.mem.readInt(u32, tmp[i..][0..4], .big);
                    if (auth_type == 3) {
                        // AuthenticationCleartextPassword
                        try sendPasswordMessage(&stream, password);
                    } else if (auth_type == 0) {
                        std.debug.print("✅ Auth ok\n", .{});
                    } else {
                        std.debug.print("⚠️ Unsupported auth type: {}\n", .{auth_type});
                    }
                },
                'S' => std.debug.print("ParameterStatus\n", .{}),
                'K' => std.debug.print("BackendKeyData\n", .{}),
                'Z' => std.debug.print("ReadyForQuery\n", .{}),
                else => std.debug.print("Unknown msg: {c}\n", .{msg_type}),
            }

            i += len - 4;
        }

        return Connection{ .stream = stream };
    }

    pub fn query(self: *Connection, sql: []const u8) !void {
        var msg = std.ArrayList(u8).init(std.heap.page_allocator);
        defer msg.deinit();

        try msg.writer().writeByte('Q');
        const len: u32 = @intCast(sql.len + 5);
        try msg.writer().writeInt(u32, len, .big);
        try msg.writer().writeAll(sql);
        try msg.writer().writeByte(0);

        try self.stream.writer().writeAll(msg.items);

        // --- Read result ---
        var buf: [4096]u8 = undefined;
        const n = try self.stream.reader().read(&buf);
        std.debug.print("Server response ({} bytes):\n{s}\n", .{ n, buf[0..n] });
    }

    pub fn close(self: *Connection) void {
        self.stream.close();
    }
};

fn writeCString(writer: anytype, s: []const u8) !void {
    try writer.writeAll(s);
    try writer.writeByte(0);
}

fn sendPasswordMessage(stream: *std.net.Stream, password: []const u8) !void {
    var msg = std.ArrayList(u8).init(std.heap.page_allocator);
    defer msg.deinit();

    try msg.writer().writeByte('p');
    const len: u32 = @intCast(password.len + 5);
    try msg.writer().writeInt(u32, len, .big);
    try msg.writer().writeAll(password);
    try msg.writer().writeByte(0);

    try stream.writer().writeAll(msg.items);
}
