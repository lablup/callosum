from callosum.serial import serial_gt, serial_lt


def test_serial_comparison():
    assert serial_lt(1, 2, bits=8)
    assert not serial_lt(2, 1, bits=8)

    assert serial_lt(255, 100, bits=8)
    assert serial_lt(255, 126, bits=8)
    assert not serial_lt(255, 127, bits=8)
    assert not serial_lt(255, 200, bits=8)

    assert not serial_gt(1, 2, bits=8)
    assert serial_gt(2, 1, bits=8)

    assert not serial_gt(255, 100, bits=8)
    assert not serial_gt(255, 127, bits=8)
    assert serial_gt(255, 128, bits=8)
    assert serial_gt(255, 200, bits=8)
