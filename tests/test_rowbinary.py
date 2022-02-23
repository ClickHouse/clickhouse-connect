from unittest import TestCase

from click_alchemy.chtypes.registry import get

def to_bytes(hex_str):
    return bytearray.fromhex(hex_str)

class TestRowBinary(TestCase):

    def test_string(self):
        str_type = get("String")
        source = to_bytes('0f 41 20 6c 6f 76 65 6c  79 20 73 74 72 69 6e 67')
        value, loc = str_type.from_row_binary(source, 0)
        assert(value == 'A lovely string')
