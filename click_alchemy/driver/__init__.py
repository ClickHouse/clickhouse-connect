from typing import Callable

type_map = dict()

class ClickHouseType:
    def __init__(self, type_name: str, to_python: Callable):
        name = type_name
        self.name = name
        self.to_python = to_python

    def to_python(self, source, start):
        return self.to_python(source, start)

import click_alchemy.driver.types