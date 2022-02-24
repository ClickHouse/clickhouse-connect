def parse_leb128(source: bytes, loc: int):
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
    length, loc = parse_leb128(source, loc)
    return source[loc:loc + length].decode(encoding), loc + length
