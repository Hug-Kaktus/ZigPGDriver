const std = @import("std");
const helpers = @import("helpers.zig");
const Connection = @import("connection.zig").Connection;
const sha256 = std.crypto.hash.sha2.Sha256;
const hmac = std.crypto.auth.hmac.sha2.HmacSha256;
const types = @import("types.zig");
const ScramState = types.ScramState;

fn sendPasswordMessage(
    self: *Connection,
    password: []const u8,
) !void {
    try self.writer.interface.writeByte('p');
    try self.writer.interface.writeInt(i32, @intCast(password.len + 5), .big);
    try self.writer.interface.writeAll(password);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

fn sendSaslInitialResponse(
    self: *Connection,
    mechanism: []const u8,
    initial: []const u8,
) !void {
    try self.writer.interface.writeByte('p');

    const len: i32 = @intCast(4 + mechanism.len + 1 + 4 + initial.len);
    try self.writer.interface.writeInt(i32, len, .big);

    try self.writer.interface.writeAll(mechanism);
    try self.writer.interface.writeByte(0);

    try self.writer.interface.writeInt(i32, @intCast(initial.len), .big);
    try self.writer.interface.writeAll(initial);

    try self.writer.interface.flush();
}

fn sendSaslResponse(self: *Connection, data: []const u8) !void {
    try self.writer.interface.writeByte('p');

    const len: i32 = @intCast(4 + data.len);
    try self.writer.interface.writeInt(i32, len, .big);

    try self.writer.interface.writeAll(data);
    try self.writer.interface.flush();
}

fn buildScramClientFirst(
    allocator: std.mem.Allocator,
    username: []const u8,
    nonce: []const u8,
) !struct {
    full: []u8,
    bare: []u8,
} {
    const bare = try std.fmt.allocPrint(
        allocator,
        "n={s},r={s}",
        .{ username, nonce },
    );

    const full = try std.fmt.allocPrint(
        allocator,
        "n,,{s}",
        .{bare},
    );

    return .{ .full = full, .bare = bare };
}

fn hi(password: []const u8, salt: []const u8, iterations: u32, out: []u8) !void {
    try std.crypto.pwhash.pbkdf2(
        out,
        password,
        salt,
        iterations,
        hmac,
    );
}

fn buildScramFinal(
    allocator: std.mem.Allocator,
    password: []const u8,
    state: *ScramState,
    server_first: []const u8,
) ![]u8 {
    try hi(password, state.salt, state.iterations, &state.salted_password);
    var client_key: [32]u8 = undefined;
    hmac.create(&client_key, "Client Key", &state.salted_password);

    var stored_key: [32]u8 = undefined;
    sha256.hash(&client_key, &stored_key, .{});

    const client_final_without_proof = try std.fmt.allocPrint(
        allocator,
        "c=biws,r={s}",
        .{state.server_nonce},
    );
    defer allocator.free(client_final_without_proof);

    state.auth_message = try std.fmt.allocPrint(
        allocator,
        "{s},{s},{s}",
        .{
            state.client_first_bare,
            server_first,
            client_final_without_proof,
        },
    );

    var signature: [32]u8 = undefined;
    hmac.create(&signature, state.auth_message, &stored_key);

    var proof: [32]u8 = undefined;
    for (&proof, 0..) |*b, i| {
        b.* = client_key[i] ^ signature[i];
    }

    var proof_buf: [std.base64.standard.Encoder.calcSize(32)]u8 = undefined;
    const proof_b64 = std.base64.standard.Encoder.encode(&proof_buf, &proof);

    return try std.fmt.allocPrint(
        allocator,
        "{s},p={s}",
        .{ client_final_without_proof, proof_b64 },
    );
}

fn buildOAuthBearer(
    allocator: std.mem.Allocator,
    token: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        allocator,
        "n,,\x01auth=Bearer {s}\x01\x01",
        .{token},
    );
}

pub const ScramSha256AuthData = struct {
    username: []const u8,
    password: []const u8,
};

pub const AuthenticationData = union {
    password: []const u8,
    scram_sha_256: ScramSha256AuthData,
};

fn parseScramServerFirst(
    allocator: std.mem.Allocator,
    msg: []const u8,
    state: *ScramState,
) !void {
    var it = std.mem.splitScalar(u8, msg, ',');

    while (it.next()) |part| {
        if (part.len < 3) continue;

        const key = part[0];
        const value = part[2..];

        switch (key) {
            'r' => {
                state.server_nonce = try allocator.dupe(u8, value);
            },
            's' => {
                const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(value) catch
                    return error.InvalidBase64;

                state.salt = try allocator.alloc(u8, decoded_len);

                try std.base64.standard.Decoder.decode(
                    @constCast(state.salt),
                    value,
                );
            },
            'i' => {
                state.iterations = try std.fmt.parseInt(u32, value, 10);
            },
            else => {
                // ignore unknown keys (future-proof)
            },
        }
    }

    if (state.server_nonce.len == 0)
        return error.MissingNonce;

    if (state.salt.len == 0)
        return error.MissingSalt;

    if (state.iterations == 0)
        return error.MissingIterations;
}

fn verifyServerSignature(
    allocator: std.mem.Allocator,
    state: *ScramState,
    msg: []const u8,
) !void {
    if (!std.mem.startsWith(u8, msg, "v=")) {
        return error.InvalidSaslFinal;
    }

    const b64 = msg[2..];

    const sig_len = try std.base64.standard.Decoder.calcSizeForSlice(b64);
    const server_sig = try allocator.alloc(u8, sig_len);
    defer allocator.free(server_sig);

    try std.base64.standard.Decoder.decode(server_sig, b64);

    var server_key: [32]u8 = undefined;
    hmac.create(&server_key, "Server Key", &state.salted_password);

    var expected: [32]u8 = undefined;
    hmac.create(&expected, state.auth_message, &server_key);

    if (!std.mem.eql(u8, &expected, server_sig)) {
        return error.InvalidServerSignature;
    }
}

pub fn authenticate(self: *Connection, payload_len: usize, password: []const u8) !void {
    const auth_type = try self.reader.interface.takeInt(i32, .big);
    switch (auth_type) {
        0 => {
            return;
        },
        3 => {
            try sendPasswordMessage(self, password);
        },
        7 => {
            std.debug.print("GSSAPI authentication is required.\n", .{});
        },
        8 => {
            var gssapi_or_sspi_auth_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer gssapi_or_sspi_auth_data.deinit(self.allocator);
            try self.reader.interface.readSliceAll(gssapi_or_sspi_auth_data.items);
            std.debug.print("gssapi_or_sspi_auth_data: {any}\n", .{gssapi_or_sspi_auth_data});
        },
        9 => {
            std.debug.print("SSPI authentication is required.\n", .{});
        },
        10 => {
            // AuthenticationSASL
            var buf = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer buf.deinit(self.allocator);
            buf.expandToCapacity();
            try self.reader.interface.readSliceAll(buf.items);

            if (std.mem.indexOf(u8, buf.items, "SCRAM-SHA-256")) |_| {
                const nonce = "randomnonce123";
                // var nonce_buf: [18]u8 = undefined;
                // std.crypto.random.bytes(&nonce_buf);

                // var nonce_b64_buf: [32]u8 = undefined;
                // const nonce = std.base64.standard.Encoder.encode(&nonce_b64_buf, &nonce_buf);
                const first = try buildScramClientFirst(self.allocator, self.user, nonce);
                defer self.allocator.free(first.full);
                defer self.allocator.free(first.bare);

                try sendSaslInitialResponse(self, "SCRAM-SHA-256", first.full);

                self.scram_state = ScramState{
                    .client_nonce = nonce,
                    .client_first_bare = try self.allocator.dupe(u8, first.bare),
                    .server_nonce = &[_]u8{},
                    .salt = &[_]u8{},
                    .iterations = 0,
                    .auth_message = undefined,
                    .salted_password = undefined,
                };
            } else if (std.mem.indexOf(u8, buf.items, "OAUTHBEARER")) |_| {
                const msg = try buildOAuthBearer(self.allocator, self.oauth_token.?);
                try sendSaslInitialResponse(self, "OAUTHBEARER", msg);
            } else {
                return error.UnsupportedAuthenticationType;
            }
        },
        11 => {
            // SASLContinue (SCRAM step 2)
            var data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer data.deinit(self.allocator);
            data.expandToCapacity();
            try self.reader.interface.readSliceAll(data.items);

            try parseScramServerFirst(self.allocator, data.items, &self.scram_state.?);

            const final = try buildScramFinal(
                self.allocator,
                password,
                &self.scram_state.?,
                data.items,
            );
            defer self.allocator.free(final);

            try sendSaslResponse(self, final);
        },
        12 => {
            var data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer data.deinit(self.allocator);
            data.expandToCapacity();
            try self.reader.interface.readSliceAll(data.items);

            try verifyServerSignature(self.allocator, &self.scram_state.?, data.items);

            return;
        },
        else => {
            return error.UnsupportedAuthenticationType;
        },
    }
}
