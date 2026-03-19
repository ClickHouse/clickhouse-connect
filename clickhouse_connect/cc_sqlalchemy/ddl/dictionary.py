from sqlalchemy import Table


# pylint: disable=abstract-method,too-many-ancestors
class Dictionary(Table):
    """
    Represents a ClickHouse Dictionary.

    Inherits from Table so it can be attached to metadata and have columns.
    """

    __visit_name__ = "dictionary"

    def __init__(self, name, metadata, *args, **kwargs):
        self.source = kwargs.pop("source", None)
        self.layout = kwargs.pop("layout", None)
        self.lifetime = kwargs.pop("lifetime", None)
        self.primary_key_def = kwargs.pop("primary_key", None)
        super().__init__(name, metadata, *args, **kwargs)
        self.kwargs["clickhouse_table_type"] = "dictionary"
        if self.source is not None:
            self.kwargs["clickhouse_dictionary_source"] = self.source
        if self.layout is not None:
            self.kwargs["clickhouse_dictionary_layout"] = self.layout
        if self.lifetime is not None:
            self.kwargs["clickhouse_dictionary_lifetime"] = self.lifetime
        if self.primary_key_def is not None:
            self.kwargs["clickhouse_dictionary_primary_key"] = self.primary_key_def
