const std = @import("std");
pub const Numeric = struct {
    negative: bool,
    scale: usize,
    digits: []usize,

    pub fn init(
        negative: bool,
        scale: usize,
        digits: []usize,
    ) Numeric {
        return .{
            .negative = negative,
            .scale = scale,
            .digits = digits,
        };
    }

    pub fn deinit(self: *Numeric, allocator: std.mem.Allocator) void {
        allocator.free(self.digits);
    }

    pub fn fromString(allocator: std.mem.Allocator, string: []const u8) !Numeric {
        const negative = if (string[0] == '-') true else false;
        const floating_point_pos = std.mem.find(u8, string, ".") orelse string.len - 1;

        const scale = string.len - 1 - floating_point_pos;
        // Is it better to exclude floating point at the beginning or ignore
        // it every time you check for symbol in string? Measure and compare
        // two different implementations.
        std.debug.print("negative: {any}\n", .{negative});
        std.debug.print("scale: {d}\n", .{scale});
        var digits_list = try std.ArrayList(usize).initCapacity(
            allocator,
            @as(
                usize,
                (@intFromFloat(@ceil(@as(f64, @floatFromInt(string.len)) / 20.0))),
            ),
        );
        var pos_in_string: usize = if (negative) 1 else 0;
        while (pos_in_string < string.len) {
            var number: usize = 0;
            var i: usize = 0;
            while (pos_in_string < string.len) : (pos_in_string += 1) {
                if (string[pos_in_string] == '.') continue;
                const digit = string[pos_in_string] - '0';
                if (digit > 9) return error.WrongFormat;

                // check overflow if yes - break inner loop
                const multiply_result = @mulWithOverflow(number, 10);
                if (multiply_result[1] == 1) {
                    break;
                }
                number = multiply_result[0];
                const add_result = @addWithOverflow(number, digit);
                if (add_result[1] == 1) {
                    break;
                }
                number = add_result[0];

                // std.debug.print("number: {d}\n", .{number});
                i += 1;
            }
            try digits_list.append(allocator, number);
        }

        std.debug.print("\n", .{});
        return Numeric{
            .negative = negative,
            .scale = scale,
            .digits = try digits_list.toOwnedSlice(allocator),
        };
    }
};

//pub fn add(a: *Numeric, b: *Numeric) !Numeric {
//     const min_len = if (a.digits.len < b.digits.len) a.digits.len else b.digits.len;
//     for (a.digits[0..min_len], b.digits[0..min_len]) |ad, bd| {}
// }

// pub fn subtract(self: *Numeric, numeric: *Numeric) !void {}
// pub fn multiply(self: *Numeric, numeric: *Numeric) !void {}
// pub fn divide(self: *Numeric, numeric: *Numeric) !void {}
