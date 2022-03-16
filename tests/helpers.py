def to_bytes(hex_str):
    return memoryview(bytes.fromhex(hex_str))