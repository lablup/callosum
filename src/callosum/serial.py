"""
A simple implementation of RFC 1982 serial number arithmetic
to handle wrapping of packet sequence numbers.

* https://tools.ietf.org/html/rfc1982

Note that when incrementing a serial number, one is allowed
to add up to (2 ** bits - 1) - 1.
"""


def serial_lt(a: int, b: int, bits: int = 32) -> bool:
    half = 2 ** (bits - 1)
    return (a < b and (b - a) < half) or (a > b and (a - b) > half)


def serial_gt(a: int, b: int, bits: int = 32) -> bool:
    half = 2 ** (bits - 1)
    return (a < b and (b - a) > half) or (a > b and (a - b) < half)


def serial_le(a: int, b: int, bits: int = 32) -> bool:
    return a == b or serial_lt(a, b, bits)


def serial_ge(a: int, b: int, bits: int = 32) -> bool:
    return a == b or serial_gt(a, b, bits)
