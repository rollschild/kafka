# Zigzag Varint Encoding

## Why zigzag encoding exists

Standard two's complement encoding is wasteful for varints when values are small negative numbers. Consider `-1`:

- As a 32-bit two's complement integer: `0xFFFFFFFF` (all 1s)
- If you naively encode this as an unsigned varint, it takes **5 bytes** (the maximum) because all those high bits are set

But `-1` is a "small" number conceptually. Zigzag encoding solves this by mapping small-magnitude numbers (whether positive or negative) to small unsigned integers, so they use fewer varint bytes.

## The mapping

Zigzag interleaves positives and negatives:

| Signed | Zigzag-encoded (unsigned) |
|--------|---------------------------|
| 0      | 0                         |
| -1     | 1                         |
| 1      | 2                         |
| -2     | 3                         |
| 2      | 4                         |
| -3     | 5                         |
| ...    | ...                       |

Now `-1` encodes as `1`, which is a single varint byte instead of five.

## The math

**Encode** (signed → unsigned):
```
encoded = (n << 1) ^ (n >> 31)    // for int32
```

- `n << 1` shifts the magnitude into the upper bits, freeing bit 0
- `n >> 31` is an arithmetic shift — produces `0x00000000` for positives, `0xFFFFFFFF` for negatives
- XOR with `0xFFFFFFFF` flips all bits (turns the shifted negative into a small positive)
- XOR with `0x00000000` does nothing (positives stay as-is, just doubled)

Walk through `-1`:
```
n = -1                          = 0xFFFFFFFF
n << 1                          = 0xFFFFFFFE
n >> 31 (arithmetic)            = 0xFFFFFFFF
0xFFFFFFFE ^ 0xFFFFFFFF         = 0x00000001 = 1  ✓
```

Walk through `1`:
```
n = 1                           = 0x00000001
n << 1                          = 0x00000002
n >> 31                         = 0x00000000
0x00000002 ^ 0x00000000         = 0x00000002 = 2  ✓
```

**Decode** (unsigned → signed):
```
decoded = (encoded >>> 1) ^ -(encoded & 1)
```

- `encoded >>> 1` is a logical (unsigned) right shift — recovers the magnitude
- `encoded & 1` extracts the sign bit (0 = positive, 1 = negative)
- `-(encoded & 1)` produces `0x00000000` or `0xFFFFFFFF`
- XOR flips all bits back for negatives, does nothing for positives

In C++:
```cpp
value = static_cast<int32_t>((raw >> 1) ^ -(raw & 1));
```

## Why Kafka uses it

Kafka record fields like `timestampDelta` and `offsetDelta` are signed integers that are typically small (often 0 or close to it). Zigzag encoding ensures these small values — positive or negative — compress into 1-2 varint bytes on the wire, saving space across millions of records.
