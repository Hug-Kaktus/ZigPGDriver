const std = @import("std");
const helpers = @import("helpers.zig");
const Connection = @import("connection.zig").Connection;

fn sendPasswordMessage(self: *Connection, password: []const u8,) !void {
    try self.writer.interface.writeByte('p');
    try self.writer.interface.writeInt(i32, @intCast(password.len + 5), .big);
    try self.writer.interface.writeAll(password);
    try self.writer.interface.writeByte(0);
    try self.writer.interface.flush();
}

pub fn authenticate(self: *Connection, payload_len: u32, password: []const u8,) !void {
    const auth_type = try self.reader.interface().readInt(i32, .big);
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
            std.debug.print("This message contains GSSAPI or SSPI data.\n", .{});
            const gssapi_or_sspi_auth_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer gssapi_or_sspi_auth_data.deinit(self.allocator);
            try self.reader.interface().appendExact(self.allocator, @constCast(&gssapi_or_sspi_auth_data), payload_len-4);
            std.debug.print("gssapi_or_sspi_auth_data: {any}\n", .{gssapi_or_sspi_auth_data});
        },
        9 => {
            std.debug.print("SSPI authentication is required.\n", .{});
        },
        10 => {
            std.debug.print("SASL authentication is required.\n", .{});
            const authentication_mechanism = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer authentication_mechanism.deinit(self.allocator);
            try self.reader.interface().appendExact(self.allocator, @constCast(&authentication_mechanism));
            std.debug.print("authentication_mechanism: {any}\n", .{authentication_mechanism});
        },
        11 => {
            std.debug.print("This message contains a SASL challenge.\n", .{});
            const sasl_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer sasl_data.deinit(self.allocator);
            try self.reader.interface().appendExact(self.allocator, @constCast(&sasl_data), payload_len-4);
            std.debug.print("sasl_data: {any}\n", .{sasl_data});
            
        },
        12 => {
            std.debug.print("SASL authentication has completed.\n", .{});
            const sasl_additional_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            const destination = sasl_additional_data.unusedCapacitySlice()[0..payload_len-4];
            _ = try self.reader.interface().readSliceShort(destination[0..]);
            std.debug.print("sasl_additional_data: {any}\n", .{sasl_additional_data});
        },
        else => {
            return error.UnsupportedAuthenticationType;
        }
    }
}
