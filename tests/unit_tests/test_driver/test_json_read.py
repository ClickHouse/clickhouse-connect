import http
from ipaddress import IPv4Address
from uuid import UUID

from urllib3 import HTTPResponse

from clickhouse_connect import datatypes
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.transform import JSONTransform
from tests.helpers import str_source
from tests.unit_tests.test_driver.binary import NESTED_BINARY

parse_response = JSONTransform().parse_response

json_resp = """
{
	"meta":
	[
		{
			"name": "query_id",
			"type": "String"
		},
		{
			"name": "user",
			"type": "LowCardinality(String)"
		},
		{
			"name": "event_time",
			"type": "DateTime"
		},
		{
			"name": "query_duration_ms",
			"type": "UInt64"
		}
	],

	"data":
	[
		["ec86bc70-00f6-46c4-b6ed-1d365a30309f", "default", "2025-04-25 09:30:56", "0"]
	],

	"rows": 1,

	"rows_before_limit_at_least": 1334,

	"statistics":
	{
		"elapsed": 0.00558552,
		"rows_read": 1334,
		"bytes_read": 64564
	}
}
"""


def test_simple_response():
    result = parse_response(HTTPResponse(json_resp, status=http.HTTPStatus.OK))
    assert result.column_names == ('query_id', 'user', 'event_time', 'query_duration_ms')
    for (inst, cls) in zip(result.column_types, (
            datatypes.string.String, datatypes.string.String, datatypes.temporal.DateTime, datatypes.numeric.UInt64)):
        assert isinstance(inst, cls)
    assert result.result_set == [['ec86bc70-00f6-46c4-b6ed-1d365a30309f',
                                  'default',
                                  '2025-04-25 09:30:56',
                                  '0']]
