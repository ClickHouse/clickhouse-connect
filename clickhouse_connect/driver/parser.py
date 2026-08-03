import re

from clickhouse_connect.driver.common import unescape_identifier

# ClickHouse lexes an unquoted identifier as a run of ASCII word characters or dollar signs,
# so names such as 9t and t$x are legal without quoting
_unquoted_identifier_re = re.compile(r"[0-9A-Za-z_$]+")


def _parse_identifier_part(expr: str, pos: int) -> int | None:
    """
    Parse a single identifier part of an identifier chain, either quoted with backticks or double quotes,
    or an unquoted sequence of identifier characters.  Returns the position immediately after the part,
    or None if there is no valid identifier part at pos.
    """
    quote = expr[pos] if pos < len(expr) else ""
    if quote not in ("`", '"'):
        match = _unquoted_identifier_re.match(expr, pos)
        return None if match is None else match.end()
    pos += 1
    length = len(expr)
    while pos < length:
        char = expr[pos]
        if char == "\\":
            # A backslash escapes the next character, for example \` or \\
            pos += 2
        elif char == quote:
            # A doubled quote character is an escaped literal, a lone one closes the part
            if pos + 1 < length and expr[pos + 1] == quote:
                pos += 2
            else:
                return pos + 1
        else:
            pos += 1
    return None


def parse_table_name(expr: str) -> tuple[str, str]:
    """
    Parse a leading, optionally database qualified, table identifier such as tbl, db.tbl, `db`.`tbl`, or
    "db"."tbl".  Whitespace around the dot separator is valid ClickHouse syntax, so it is accepted and
    preserved in the returned identifier.

    The identifier must be followed by the end of the expression or by a delimiter, so an expression such
    as db-tbl is rejected rather than parsed as the identifier db.

    :param expr: Expression starting with a table identifier
    :return: Tuple of the identifier as written and any text remaining after it
    :raises ValueError: If expr does not start with a valid table identifier
    """
    pos = len(expr) - len(expr.lstrip())
    start = pos
    while True:
        part_end = _parse_identifier_part(expr, pos)
        if part_end is None:
            raise ValueError(f"Unrecognized table name in {expr}")
        end = part_end
        pos = end
        while pos < len(expr) and expr[pos].isspace():
            pos += 1
        if pos >= len(expr) or expr[pos] != ".":
            if end < len(expr) and not expr[end].isspace() and expr[end] not in "(,;":
                raise ValueError(f"Unrecognized table name in {expr}")
            return expr[start:end], expr[end:].strip()
        pos += 1
        while pos < len(expr) and expr[pos].isspace():
            pos += 1


def parse_callable(expr) -> tuple[str, tuple[str | int, ...], str]:
    """
    Parses a single level ClickHouse optionally 'callable' function/identifier.  The identifier is returned as the
    first value in the response tuple.  If the expression is callable -- i.e. an identifier followed by 0 or more
    arguments in parentheses, the second returned value is a tuple of the comma separated arguments.  The third and
    final tuple value is any text remaining after the initial expression for further parsing/processing.

    Examples:
      "Tuple(String, Enum('one' = 1, 'two' = 2))" will return "Tuple", ("String", "Enum('one' = 1,'two' = 2)"), ""
      "MergeTree() PARTITION BY key" will return "MergeTree", (), "PARTITION BY key"

    :param expr:  ClickHouse DDL or Column Name expression
    :return: Tuple of the identifier, a tuple of arguments, and remaining text
    """
    expr = expr.strip()
    pos = expr.find("(")
    space = expr.find(" ")
    if pos == -1 and space == -1:
        return expr, (), ""
    if space != -1 and (pos == -1 or space < pos):
        return expr[:space], (), expr[space:].strip()
    name = expr[:pos]
    pos += 1  # Skip first paren
    values = []
    value = ""
    in_str = False
    level = 0

    def add_value():
        try:
            values.append(int(value))
        except ValueError:
            values.append(value)

    while True:
        char = expr[pos]
        pos += 1
        if in_str:
            value += char
            if char == "'":
                in_str = False
            elif char == "\\" and expr[pos] == "'" and expr[pos : pos + 4] != "' = " and expr[pos : pos + 2] != "')":
                value += expr[pos]
                pos += 1
        else:
            if level == 0:
                if char == " ":
                    space = pos
                    temp_char = expr[space]
                    while temp_char == " ":
                        space += 1
                        temp_char = expr[space]
                    if not value or temp_char in "()',=><0":
                        char = temp_char
                        pos = space + 1
                if char == ",":
                    add_value()
                    value = ""
                    continue
                if char == ")":
                    break
            if char == "'" and (not value or "Enum" in value):
                in_str = True
            elif char == "(":
                level += 1
            elif char == ")" and level:
                level -= 1
            value += char
    if value != "":
        add_value()
    return name, tuple(values), expr[pos:].strip()


def parse_enum(expr) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """
    Parse a ClickHouse enum definition expression of the form ('key1' = 1, 'key2' = 2)
    :param expr: ClickHouse enum expression/arguments
    :return: Parallel tuples of string enum keys and integer enum values
    """
    keys = []
    values = []
    pos = expr.find("(") + 1
    in_key = False
    key: list[str] = []
    value: list[str] = []
    while True:
        char = expr[pos]
        pos += 1
        if in_key:
            if char == "'":
                keys.append("".join(key))
                key = []
                in_key = False
            elif char == "\\" and expr[pos] == "'" and expr[pos : pos + 4] != "' = " and expr[pos:] != "')":
                key.append(expr[pos])
                pos += 1
            else:
                key.append(char)
        elif char not in (" ", "="):
            if char == ",":
                values.append(int("".join(value)))
                value = []
            elif char == ")":
                values.append(int("".join(value)))
                break
            elif char == "'" and not value:
                in_key = True
            else:
                value.append(char)
    sorted_values, sorted_keys = zip(*sorted(zip(values, keys)))
    return tuple(sorted_keys), tuple(sorted_values)


def parse_columns(expr: str):
    """
    Parse a ClickHouse column list of the form (col1 String, col2 Array(Tuple(String, Int32))).  This also handles
    unnamed columns (such as Tuple definitions).  Mixed named and unnamed columns are not currently supported.
    :param expr: ClickHouse enum expression/arguments
    :return: Parallel tuples of column types and column types (strings)
    """
    names = []
    columns = []
    pos = 1
    named = False
    level = 0
    label = ""
    quote = None
    while True:
        char = expr[pos]
        pos += 1
        if quote:
            if char == quote:
                quote = None
            elif char == "\\" and expr[pos] == "'" and expr[pos : pos + 4] != "' = " and expr[pos : pos + 2] != "')":
                label += char + expr[pos]
                pos += 1
                continue
        else:
            if level == 0:
                if char in (" ", "="):
                    if label and not named:
                        names.append(unescape_identifier(label))
                        label = ""
                        named = True
                    char = ""
                elif char == ",":
                    columns.append(label)
                    named = False
                    label = ""
                    continue
                elif char == ")":
                    columns.append(label)
                    break
            if char in ("'", "`") and (not label or "Enum" in label):
                quote = char
            elif char == "(":
                level += 1
            elif char == ")":
                level -= 1
        label += char
    return tuple(names), tuple(columns)
