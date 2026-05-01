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
) ![]u8 {
    // "n,,n=user,r=nonce"
    return try std.fmt.allocPrint(
        allocator,
        "n,,n={s},r={s}",
        .{ username, nonce },
    );
}

fn hi(password: []const u8, salt: []const u8, iterations: u32, out: []u8) !void {
    // PBKDF2-HMAC-SHA256
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
    client_first_bare: []const u8,
    server_first: []const u8,
) ![]u8 {
    var salted: [32]u8 = undefined;
    try hi(password, state.salt, state.iterations, &salted);

    // client key
    var client_key: [32]u8 = undefined;
    hmac.create(&client_key, &salted, "Client Key");

    // stored key
    var stored_key: [32]u8 = undefined;
    sha256.hash(&client_key, &stored_key, .{});

    // auth message
    const auth_msg = try std.fmt.allocPrint(
        allocator,
        "{s},{s},c=biws,r={s}",
        .{ client_first_bare, server_first, state.server_nonce },
    );

    // client signature
    var signature: [32]u8 = undefined;
    hmac.create(&signature, &stored_key, auth_msg);

    // proof = client_key XOR signature
    var proof: [32]u8 = undefined;
    for (&proof, 0..) |*b, i| {
        b.* = client_key[i] ^ signature[i];
    }

    var proof_buf: [std.base64.standard.Encoder.calcSize(32)]u8 = undefined;
    const proof_b64 = std.base64.standard.Encoder.encode(&proof_buf, &proof);

    return try std.fmt.allocPrint(
        allocator,
        "c=biws,r={s},p={s}",
        .{ state.server_nonce, proof_b64 },
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

pub fn authenticate(self: *Connection, payload_len: usize, password: []const u8) !void {
    const auth_type = try self.reader.interface().takeInt(i32, .big);
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
            try self.reader.interface().readSliceAll(gssapi_or_sspi_auth_data.items);
            std.debug.print("gssapi_or_sspi_auth_data: {any}\n", .{gssapi_or_sspi_auth_data});
        },
        9 => {
            std.debug.print("SSPI authentication is required.\n", .{});
        },
        10 => {
            // AuthenticationSASL
            var buf = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer buf.deinit(self.allocator);
            try self.reader.interface().readSliceAll(buf.items);

            if (std.mem.indexOf(u8, buf.items, "scram-sha-256")) |_| {
                const nonce = "randomnonce123"; // TODO: generate securely

                const first = try buildScramClientFirst(self.allocator, self.user, nonce);

                try sendSaslInitialResponse(self, "SCRAM-SHA-256", first);

                self.scram_state = ScramState{
                    .client_nonce = nonce,
                    .server_nonce = &[_]u8{},
                    .salt = &[_]u8{},
                    .iterations = 0,
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
            try self.reader.interface().readSliceAll(data.items);

            // TODO: parse r,s,i into self.scram_state

            const final = try buildScramFinal(
                self.allocator,
                password,
                &self.scram_state.?,
                "client-first-bare",
                data.items,
            );

            try sendSaslResponse(self, final);
        },
        12 => {
            var sasl_additional_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len - 4);
            defer sasl_additional_data.deinit(self.allocator);
            try self.reader.interface().readSliceAll(sasl_additional_data.items);
            std.debug.print("sasl_additional_data: {any}\n", .{sasl_additional_data});
            // SASLFinal
            // verify server signature (optional but correct)
            return;
        },
        else => {
            return error.UnsupportedAuthenticationType;
        },
    }
}
