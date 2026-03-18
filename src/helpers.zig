const std = @import("std");

/// Builds human readable message from server's ErrorResponse or NoticeResponse messages
pub fn buildMessage(allocator: std.mem.Allocator, reader: *std.Io.Reader) !std.ArrayList(u8) {
    var field_type: u8 = undefined;
    std.debug.print("buildMessage>before allocation> reader.buffer: {any}\n", .{reader.buffer});
    var message = try std.ArrayList(u8).initCapacity(allocator, 128);
    std.debug.print("buildMessage>after allocation> reader.buffer: {any}\n", .{reader.buffer});
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

pub fn parseKeyValuePayload(payload: []const u8) !std.StringHashMap([]const u8) {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var parameters = std.StringHashMap([]const u8).init(allocator);
    var i: u16 = 0;
    var j: u16 = 0;
    var is_key = true;
    var key: []const u8 = undefined;
    while (true) {
        if (payload[i] != 0) {
            i += 1;
        } else {
            if (is_key) {
                key = payload[j..i];
            } else {
                try parameters.put(key, payload[j..i]);
            }
            is_key = !is_key;
            i += 1;
            j = i;
        }
        if (i >= payload.len) return parameters;
    }
}
