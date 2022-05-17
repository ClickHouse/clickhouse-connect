from typing import Union, Tuple


# pylint: disable=too-many-branches
def parse_callable(expr) -> Tuple[str, Tuple[Union[str, int], ...], str]:
    """
    Parses a single level ClickHouse optionally 'callable' function/identifier.  The identifier is returned as the first value in
    the response tuple.  If the expression is callable -- i.e. an identifier followed by 0 or more arguments in parentheses, the
    second returned value is a tuple of the comma separated arguments.  Unquoted spaces in the arguments will be removed.  The third and
    final tuple value is any text remaining after the initial expression for further parsing/processing.

    Examples:
      "Tuple(String, Enum('one' = 1, 'two' = 2))" will return "Tuple", ("String", "Enum('one'=1,'two'=2)"), ""
      "MergeTree() PARTITION BY key" will return "MergeTree", (), "PARTITION BY key"

    :param expr:  ClickHouse DDL or Column Name expression
    :return: Tuple of the identifier, a tuple of arguments, and remaining text
    """
    expr = expr.strip()
    pos = expr.find('(')
    space = expr.find(' ')
    if pos == -1 and space == -1:
        return expr, (), ''
    if space != -1 and (pos == -1 or space < pos):
        return expr[:space], (), expr[space:].strip()
    name = expr[:pos]
    pos += 1  # Skip first paren
    values = []
    value = ''
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
            elif char == '\\' and expr[pos] == "'" and expr[pos:pos + 4] != "' = " and expr[pos:pos + 2] != "')":
                value += expr[pos]
                pos += 1
        else:
            if not level:
                while char == ' ':
                    char = expr[pos]
                    pos += 1
                if char == ',':
                    add_value()
                    value = ''
                    continue
                if char == ')':
                    break
            if char == "'" and (not value or 'Enum' in value):
                in_str = True
            elif char == '(':
                level += 1
            elif char == ')' and level:
                level -= 1
            value += char
    if value != '':
        add_value()
    return name, tuple(values), expr[pos:].strip()


def parse_enum(expr) -> Tuple[Tuple[str], Tuple[int]]:
    """
    Parse a ClickHouse enum definition expression of the form ('key1' = 1, 'key2' = 2)
    :param expr: ClickHouse enum expression/arguments
    :return: Parallel tuples of string enum keys and integer enum values
    """
    keys = []
    values = []
    pos = expr.find('(') + 1
    in_key = False
    key = ''
    value = ''
    while True:
        char = expr[pos]
        pos += 1
        if in_key:
            if char == "'":
                keys.append(key)
                key = ''
                in_key = False
            elif char == '\\' and expr[pos] == "'" and expr[pos:pos + 4] != "' = " and expr[pos:] != "')":
                key += expr[pos]
                pos += 1
            else:
                key += char
        elif char not in (' ', '='):
            if char == ',':
                values.append(int(value))
                value = ''
            elif char == ')':
                values.append(int(value))
                break
            elif char == "'" and not value:
                in_key = True
            else:
                value += char
    values, keys = zip(*sorted(zip(values, keys)))
    return tuple(keys), tuple(values)
