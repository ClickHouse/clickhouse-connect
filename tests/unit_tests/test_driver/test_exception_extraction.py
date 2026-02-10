from clickhouse_connect.driver.transform import (
    extract_error_message,
    extract_exception_with_tag,
)


# pylint: disable=no-self-use
class TestExceptionExtraction:
    """Tests for exception message extraction with the new tag format"""

    def test_extract_exception_with_tag_basic(self):
        """Test extracting a simple exception with the new format"""
        exception_tag = "PU1FNUFH98"
        error_msg = "Big bam occurred right while reading the data"

        # Format: __exception__<TAG>\r\n<error message>\r\n<message_length> <TAG>__exception__\r\n
        response_body = (
            b"bodybodybodybody\r\n"
            b"__exception__PU1FNUFH98\r\n"
            b"Big bam occurred right while reading the data\r\n"
            b"46 PU1FNUFH98__exception__\r\n"
        )

        result = extract_exception_with_tag(response_body, exception_tag)
        assert result is not None
        assert error_msg in result

    def test_extract_exception_with_tag_multiline_error(self):
        """Test extracting an exception with multiple lines in the error message"""
        exception_tag = "ABC1234567"
        error_msg_part1 = "Error on line 1"
        error_msg_part2 = "Error on line 2"

        response_body = (
            b"__exception__ABC1234567\r\n"
            b"Error on line 1\n"
            b"Error on line 2\r\n"
            b"99 ABC1234567__exception__\r\n"
        )

        result = extract_exception_with_tag(response_body, exception_tag)
        assert result is not None
        assert error_msg_part1 in result
        assert error_msg_part2 in result

    def test_extract_error_message_fallback(self):
        """Test that the old extract_error_message still works for backwards compatibility"""
        response_body = b"Code: 60. DB::Exception: Table default.test doesn't exist"

        result = extract_error_message(response_body)
        assert "Code: 60" in result
        assert "doesn't exist" in result
