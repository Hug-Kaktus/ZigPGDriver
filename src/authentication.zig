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

pub fn authenticate(self: *Connection, payload_len: usize, password: []const u8,) !void {
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
            var gssapi_or_sspi_auth_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer gssapi_or_sspi_auth_data.deinit(self.allocator);
            try self.reader.interface().readSliceAll(gssapi_or_sspi_auth_data.items);
            std.debug.print("gssapi_or_sspi_auth_data: {any}\n", .{gssapi_or_sspi_auth_data});
        },
        9 => {
            std.debug.print("SSPI authentication is required.\n", .{});
        },
        10 => {
            var authentication_mechanism = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer authentication_mechanism.deinit(self.allocator);
            try self.reader.interface().readSliceAll(authentication_mechanism.items);
            std.debug.print("authentication_mechanism: {any}\n", .{authentication_mechanism});
        },
        11 => {
            var sasl_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer sasl_data.deinit(self.allocator);
            try self.reader.interface().readSliceAll(sasl_data.items);
            std.debug.print("sasl_data: {any}\n", .{sasl_data});
        },
        12 => {
            var sasl_additional_data = try std.ArrayList(u8).initCapacity(self.allocator, payload_len-4);
            defer sasl_additional_data.deinit(self.allocator);
            try self.reader.interface().readSliceAll(sasl_additional_data.items);
            std.debug.print("sasl_additional_data: {any}\n", .{sasl_additional_data});
        },
        else => {
            return error.UnsupportedAuthenticationType;
        }
    }
}
