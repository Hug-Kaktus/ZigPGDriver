const std = @import("std");
const Connection = @import("connection.zig").Connection;

pub fn copy(self: *Connection, data: []const u8) !void {
    try self.writer.interface.writeByte('d');
    try self.writer.interface.writeInt(i32, 4 + data.len, .big);
    try self.writer.interface.write(data);
    try self.writer.interface.flush();
}
