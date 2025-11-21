import math
import random
from typing import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from tests.integration_tests.conftest import TestConfig


@pytest.fixture(autouse=True, scope="module")
def module_setup_and_checks(test_client: Client, test_config: TestConfig):
    """
    Performs all module-level setup:
    - Skips if in a cloud environment where experimental settings are locked.
    - Skips if the server version is too old for QBit types.
    """
    if test_config.cloud:
        pytest.skip(
            "QBit type requires allow_experimental_qbit_type setting, but settings are locked in cloud, skipping tests.",
            allow_module_level=True,
        )

    if not test_client.min_version("25.10"):
        pytest.skip("QBit type requires ClickHouse 25.10+", allow_module_level=True)


def test_qbit_roundtrip_float64(test_client: Client, table_context: Callable):
    """Test QBit(Float64) round-trip accuracy with fruit_animal example data"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("fruit_animal", ["word String", "vec QBit(Float64, 5)"]):
        test_data = [
            ("apple", [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
            ("banana", [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
            ("orange", [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
        ]

        test_client.insert("fruit_animal", test_data)
        count = test_client.query("SELECT COUNT(*) FROM fruit_animal").result_set[0][0]

        assert count == 3

        for word, original_vec in test_data:
            result = test_client.query("SELECT vec FROM fruit_animal WHERE word = %(word)s", parameters={"word": word})
            retrieved_vec = result.result_set[0][0]

            assert isinstance(retrieved_vec, list)
            assert len(retrieved_vec) == 5
            assert retrieved_vec == original_vec


def test_qbit_roundtrip_float32(test_client: Client, table_context: Callable):
    """Test QBit(Float32) round-trip accuracy"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("vectors_f32", ["id Int32", "vec QBit(Float32, 8)"]):
        test_data = [
            (1, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
            (2, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
            (3, [-1.5, -2.5, -3.5, -4.5, -5.5, -6.5, -7.5, -8.5]),
        ]

        test_client.insert("vectors_f32", test_data)

        for id_val, original_vec in test_data:
            result = test_client.query("SELECT vec FROM vectors_f32 WHERE id = %(id)s", parameters={"id": id_val})
            retrieved_vec = result.result_set[0][0]

            assert isinstance(retrieved_vec, list)
            assert len(retrieved_vec) == 8
            assert retrieved_vec == pytest.approx(original_vec, rel=1e-6)


def test_qbit_roundtrip_bfloat16(test_client: Client, table_context: Callable):
    """Test QBit(BFloat16) round-trip with appropriate tolerance"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("vectors_bf16", ["id Int32", "vec QBit(BFloat16, 8)"]):
        test_data = [
            (1, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
            (2, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]),
        ]

        test_client.insert("vectors_bf16", test_data)

        for id_val, original_vec in test_data:
            result = test_client.query("SELECT vec FROM vectors_bf16 WHERE id = %(id)s", parameters={"id": id_val})
            retrieved_vec = result.result_set[0][0]

            assert isinstance(retrieved_vec, list)
            assert len(retrieved_vec) == 8
            assert retrieved_vec == pytest.approx(original_vec, rel=1e-2, abs=1e-2)


def test_qbit_distance_search(test_client: Client, table_context: Callable):
    """Test L2DistanceTransposed with different precision levels"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("fruit_animal", ["word String", "vec QBit(Float64, 5)"]):
        test_data = [
            ("apple", [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
            ("banana", [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
            ("orange", [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
            ("dog", [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
            ("cat", [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
            ("horse", [-0.61435682, 0.4851571, 1.21091247, -0.62530446, -1.33082533]),
        ]

        test_client.insert("fruit_animal", test_data)

        # Search for "lemon" vector
        lemon_vector = [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770]

        # Full precision search (64-bit)
        full_precision = test_client.query(
            """
            SELECT word, L2DistanceTransposed(vec, %(lemon)s, 64) AS distance
            FROM fruit_animal
            ORDER BY distance ASC
            """,
            parameters={"lemon": lemon_vector},
        )

        assert full_precision.result_set[0][0] == "apple"
        apple_distance = full_precision.result_set[0][1]
        assert apple_distance == pytest.approx(0.1464, abs=1e-3)

        # Reduced precision search
        reduced_precision = test_client.query(
            """
            SELECT word, L2DistanceTransposed(vec, %(lemon)s, 12) AS distance
            FROM fruit_animal
            ORDER BY distance ASC
            """,
            parameters={"lemon": lemon_vector},
        )

        assert reduced_precision.result_set[0][0] == "apple"
        assert reduced_precision.result_set[0][1] > 0


def test_qbit_batch_insert(test_client: Client, table_context: Callable):
    """Test batch insert with multiple vectors"""

    test_client.command("SET allow_experimental_qbit_type = 1")
    dimension = 16

    with table_context("embeddings", ["id Int32", f"embedding QBit(Float32, {dimension})"]):
        random.seed(1)

        batch_data = []
        for i in range(100):
            vector = [random.uniform(-1.0, 1.0) for _ in range(dimension)]
            batch_data.append((i, vector))

        test_client.insert("embeddings", batch_data)

        count = test_client.query("SELECT COUNT(*) FROM embeddings").result_set[0][0]
        assert count == 100

        # Spot check a few
        for test_id in [0, 50, 99]:
            original_vec = batch_data[test_id][1]
            result = test_client.query("SELECT embedding FROM embeddings WHERE id = %(id)s", parameters={"id": test_id})
            retrieved_vec = result.result_set[0][0]

            assert len(retrieved_vec) == dimension
            assert retrieved_vec == pytest.approx(original_vec, rel=1e-6)


def test_qbit_null_handling(test_client: Client, table_context: Callable):
    """Test QBit with NULL values using Nullable wrapper"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("nullable_vecs", ["id Int32", "vec Nullable(QBit(Float32, 4))"]):
        test_data = [
            (1, [1.0, 2.0, 3.0, 4.0]),
            (2, None),
            (3, [5.0, 6.0, 7.0, 8.0]),
        ]

        test_client.insert("nullable_vecs", test_data)

        result = test_client.query("SELECT id, vec FROM nullable_vecs ORDER BY id")
        assert result.result_set[0][1] == pytest.approx([1.0, 2.0, 3.0, 4.0])
        assert result.result_set[1][1] is None
        assert result.result_set[2][1] == pytest.approx([5.0, 6.0, 7.0, 8.0])


def test_qbit_dimension_mismatch_error(test_client: Client, table_context: Callable):
    """Test that inserting vectors with wrong dimensions raises an error"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("dim_test", ["id Int32", "vec QBit(Float32, 8)"]):
        wrong_data = [(1, [1.0, 2.0, 3.0, 4.0, 5.0])]

        with pytest.raises(ValueError) as exc_info:
            test_client.insert("dim_test", wrong_data)

        assert "dimension mismatch" in str(exc_info.value).lower()


def test_qbit_empty_insert(test_client: Client, table_context: Callable):
    """Test inserting an empty list (no rows)"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("empty_test", ["id Int32", "vec QBit(Float32, 4)"]):
        test_client.insert("empty_test", [])
        result = test_client.query("SELECT COUNT(*) FROM empty_test")
        assert result.result_set[0][0] == 0


def test_qbit_single_row(test_client: Client, table_context: Callable):
    """Test inserting a single row"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("single_row", ["id Int32", "vec QBit(Float32, 4)"]):
        single_data = [(1, [1.0, 2.0, 3.0, 4.0])]
        test_client.insert("single_row", single_data)

        result = test_client.query("SELECT id, vec FROM single_row")
        assert len(result.result_set) == 1
        assert result.result_set[0][0] == 1
        assert result.result_set[0][1] == pytest.approx([1.0, 2.0, 3.0, 4.0])


def test_qbit_special_float_values(test_client: Client, table_context: Callable):
    """Test QBit with special float values (inf, -inf, nan)"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("special_floats", ["id Int32", "vec QBit(Float64, 4)"]):
        test_data = [
            (1, [float("inf"), 1.0, 2.0, 3.0]),
            (2, [float("-inf"), 1.0, 2.0, 3.0]),
            (3, [float("nan"), 1.0, 2.0, 3.0]),
            (4, [0.0, -0.0, 1.0, -1.0]),
        ]

        test_client.insert("special_floats", test_data)
        result = test_client.query("SELECT id, vec FROM special_floats ORDER BY id")

        assert result.result_set[0][1][0] == float("inf")
        assert result.result_set[0][1][1] == pytest.approx(1.0)

        assert result.result_set[1][1][0] == float("-inf")
        assert result.result_set[1][1][1] == pytest.approx(1.0)

        assert math.isnan(result.result_set[2][1][0])
        assert result.result_set[2][1][1] == pytest.approx(1.0)

        assert result.result_set[3][1][0] == 0.0
        assert result.result_set[3][1][1] == -0.0 or result.result_set[3][1][1] == 0.0


def test_qbit_edge_case_dimensions(test_client: Client, table_context: Callable):
    """Test QBit with edge case dimensions (1, not multiple of 8)"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("dim_one", ["id Int32", "vec QBit(Float32, 1)"]):
        test_data = [(1, [1.0]), (2, [3.14])]
        test_client.insert("dim_one", test_data)

        result = test_client.query("SELECT vec FROM dim_one ORDER BY id")
        assert result.result_set[0][0] == pytest.approx([1.0])
        assert result.result_set[1][0] == pytest.approx([3.14])

    with table_context("dim_five", ["id Int32", "vec QBit(Float32, 5)"]):
        test_data = [(1, [1.0, 2.0, 3.0, 4.0, 5.0])]
        test_client.insert("dim_five", test_data)

        result = test_client.query("SELECT vec FROM dim_five")
        assert result.result_set[0][0] == pytest.approx([1.0, 2.0, 3.0, 4.0, 5.0])


def test_qbit_very_large_batch(test_client: Client, table_context: Callable):
    """Test inserting a very large batch of vectors (1000 rows)"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("large_batch", ["id Int32", "vec QBit(Float32, 8)"]):
        random.seed(1)
        large_batch = [(i, [random.uniform(-10, 10) for _ in range(8)]) for i in range(1000)]

        test_client.insert("large_batch", large_batch)

        count = test_client.query("SELECT COUNT(*) FROM large_batch").result_set[0][0]
        assert count == 1000

        for check_id in [0, 500, 999]:
            original_vec = large_batch[check_id][1]
            result = test_client.query("SELECT vec FROM large_batch WHERE id = %(id)s", parameters={"id": check_id})
            retrieved_vec = result.result_set[0][0]
            assert retrieved_vec == pytest.approx(original_vec, rel=1e-6)


def test_qbit_all_nulls(test_client: Client, table_context: Callable):
    """Test QBit nullable column with all NULL values"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("all_nulls", ["id Int32", "vec Nullable(QBit(Float32, 4))"]):
        test_data = [(1, None), (2, None), (3, None)]
        test_client.insert("all_nulls", test_data)

        result = test_client.query("SELECT vec FROM all_nulls ORDER BY id")
        assert all(row[0] is None for row in result.result_set)


def test_qbit_all_zeros(test_client: Client, table_context: Callable):
    """Test QBit with all zero vectors"""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with table_context("all_zeros", ["id Int32", "vec QBit(Float32, 4)"]):
        test_data = [(1, [0.0, 0.0, 0.0, 0.0]), (2, [0.0, 0.0, 0.0, 0.0])]
        test_client.insert("all_zeros", test_data)

        result = test_client.query("SELECT vec FROM all_zeros ORDER BY id")
        assert result.result_set[0][0] == pytest.approx([0.0, 0.0, 0.0, 0.0])
        assert result.result_set[1][0] == pytest.approx([0.0, 0.0, 0.0, 0.0])


def test_invalid_dimension(test_client: Client, table_context: Callable):
    """Try creating a column with a negative dimension."""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with pytest.raises(DatabaseError):
        with table_context("bad_dim", ["id Int32", "vec QBit(Float32, -8)"]):
            pass


def test_invalid_element_type(test_client: Client, table_context: Callable):
    """Try creating a column with an invalid element type."""

    test_client.command("SET allow_experimental_qbit_type = 1")

    with pytest.raises(DatabaseError):
        with table_context("bad_el_type", ["id Int32", "vec QBit(Int32, 8)"]):
            pass
