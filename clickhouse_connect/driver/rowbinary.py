def read_leb128(source: bytes, loc: int):
    length = 0
    ix = 0
    while True:
        b = source[loc + ix]
        length = length + ((b & 0x7f) << (ix * 7))
        ix += 1
        if (b & 0x80) == 0:
            break
    return length, loc + ix


def string_leb128(source: bytes, loc: int, encoding: str = 'utf8'):
    length, loc = read_leb128(source, loc)
    return source[loc:loc + length].decode(encoding), loc + length


def write_leb128(dest: bytearray, value: int):  #Unsigned only
    while True:
        b = value & 0x7f
        value = value >> 7
        if value == 0:
            dest.append(b)
            return
        dest.append(0x80 | b)


def write_string(dest: bytearray, value: str):
    write_leb128(dest, len(value))
    dest += bytes(value)

