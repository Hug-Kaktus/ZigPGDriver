const std = @import("std");
const Connection = @import("connection.zig").Connection;

/// Builds human readable message from server's ErrorResponse or NoticeResponse messages
pub fn buildMessage(allocator: std.mem.Allocator, reader: *std.Io.Reader) !std.ArrayList(u8) {
    var field_type: u8 = undefined;
    var message = try std.ArrayList(u8).initCapacity(allocator, 128);
    while (true) {
        field_type = try reader.takeByte();
        switch (field_type) {
            'S' => try message.appendSlice(allocator, "Severity: "),
            'V' => try message.appendSlice(allocator, "Severity localized: "),
            'C' => try message.appendSlice(allocator, "Code: "),
            'M' => try message.appendSlice(allocator, "Message: "),
            'D' => try message.appendSlice(allocator, "Detail: "),
            'H' => try message.appendSlice(allocator, "Hint: "),
            'P' => try message.appendSlice(allocator, "Position: "),
            'p' => try message.appendSlice(allocator, "Internal position: "),
            'q' => try message.appendSlice(allocator, "Internal query: "),
            'W' => try message.appendSlice(allocator, "Where: "),
            's' => try message.appendSlice(allocator, "Schema name: "),
            't' => try message.appendSlice(allocator, "Table name: "),
            'c' => try message.appendSlice(allocator, "Column name: "),
            'd' => try message.appendSlice(allocator, "Data type name: "),
            'n' => try message.appendSlice(allocator, "Constraint name: "),
            'F' => try message.appendSlice(allocator, "File: "),
            'L' => try message.appendSlice(allocator, "Line: "),
            'R' => try message.appendSlice(allocator, "Routine: "),
            0 => break,
            else => {
                _ = try reader.takeDelimiter(0);
                continue;
            }
        }
        try message.appendSlice(allocator, (try reader.takeDelimiter(0)).?);
        try message.appendSlice(allocator, "\n");
    }
    return message;
}

pub fn parseKeyValuePayload(self: *Connection) !void {
    const key = (try self.reader.interface().takeDelimiter(0)).?;
    const value = (try self.reader.interface().takeDelimiter(0)).?;
    switch (key.len) {
        8 => {
            self.backend_key_data.TimeZone = value;
        },
        9 => {
            self.backend_key_data.DateStyle = value;
        },
        11 => {
            self.backend_key_data.search_path = value;
        },
        12 => {
            self.backend_key_data.is_superuser = value;
        },
        13 => {
            self.backend_key_data.IntervalStyle = value;
        },
        14 => {
            self.backend_key_data.in_hot_standby = value;
        },
        15 => {
            if (std.mem.eql(u8, "client_encoding", value)) {
                self.backend_key_data.client_encoding = value;
            } else {
                self.backend_key_data.server_encoding = value;
            }
        },
        16 => {
            if (std.mem.eql(u8, "application_name", value)) {
                self.backend_key_data.application_name = value;
            } else {
                self.backend_key_data.scram_iterations = value;
            }
        },
        17 => {
            self.backend_key_data.integer_datetimes = value;
        },
        21 => {
            self.backend_key_data.session_authorization = value;
        },
        27 => {
            self.backend_key_data.standard_conforming_strings = value;
        },
        29 => {
            self.backend_key_data.default_transaction_read_only = value;
        },
        else => {
            std.debug.print("Unknown backend key parameter \"{s}\" is ignored.", .{value});
        }
    }
}
