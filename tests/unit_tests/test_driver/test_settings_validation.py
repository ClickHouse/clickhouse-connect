"""Unit tests for the settings validation fix for issue #638"""
from unittest.mock import Mock, patch
import pytest

from clickhouse_connect.driver.models import SettingDef


class TestSettingsValidation:
    """Test the _validate_setting method behavior for issue #638

    Issue: Can't explicitly send "redundant" settings that match server defaults

    The fix ensures that settings are only skipped if they're already explicitly
    set on the client with the same value. This allows users to explicitly set
    a value (even if it's the default) and have it sent to the server.
    """

    def setup_method(self):
        """Set up a mock client with server settings"""
        # Create a mock client with the necessary attributes and methods
        self.client = Mock()
        self.client.valid_transport_settings = set()
        self.client.optional_transport_settings = set()

        # Mock server settings with some defaults
        self.server_settings = {
            'do_not_merge_across_partitions_select_final': SettingDef(
                name='do_not_merge_across_partitions_select_final',
                value='0',
                readonly=0
            ),
            'max_threads': SettingDef(
                name='max_threads',
                value='0',  # Auto (default)
                readonly=0
            ),
            'some_custom_setting': SettingDef(
                name='some_custom_setting',
                value='default_value',
                readonly=0
            ),
        }

        # Set up client settings storage
        self.client_settings = {}

        def get_client_setting(key):
            return self.client_settings.get(key)

        self.client.get_client_setting = get_client_setting
        self.client.server_settings = self.server_settings

        # Import the actual _validate_setting method
        from clickhouse_connect.driver.client import Client
        self.validate_method = Client._validate_setting

    def test_setting_matching_default_not_set_on_client_is_sent(self):
        """Test that a setting matching server default IS sent if not already set on client

        This is the core fix for issue #638. Before the fix, settings that matched
        server defaults were silently skipped even if the user explicitly set them.
        This was problematic for settings like do_not_merge_across_partitions_select_final
        that can be dynamically changed by the server if not explicitly set.
        """
        # Setting is NOT set on client (get_client_setting returns None)
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            0,  # Matches server default of '0'
            'error'  # invalid_action
        )
        # Should return the string value, not None (meaning it WILL be sent)
        assert result == '0'

    def test_setting_matching_default_already_set_on_client_is_skipped(self):
        """Test that a setting matching server default is NOT sent if already set on client

        If the setting is already explicitly set on the client to the same value,
        we don't need to send it again.
        """
        # Setting IS already set on client to the same value
        self.client_settings['do_not_merge_across_partitions_select_final'] = '0'
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            0,  # Matches server default of '0'
            'error'  # invalid_action
        )
        # Should return None (meaning it will NOT be sent)
        assert result is None

    def test_setting_different_from_default_is_sent(self):
        """Test that a setting different from server default IS always sent"""
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            1,  # Different from server default of '0'
            'error'
        )
        # Should return the string value
        assert result == '1'

    def test_setting_different_from_current_client_value_is_sent(self):
        """Test that a setting is sent when changing from current client value"""
        # Setting is set on client to a different value
        self.client_settings['do_not_merge_across_partitions_select_final'] = '1'
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            0,  # Different from current client value '1'
            'error'
        )
        # Should return the string value (new value should be sent)
        assert result == '0'

    def test_boolean_true_converted_to_1(self):
        """Test that boolean True is converted to '1'"""
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            True,
            'error'
        )
        assert result == '1'

    def test_boolean_false_converted_to_0(self):
        """Test that boolean False is converted to '0'"""
        result = self.validate_method(
            self.client,
            'do_not_merge_across_partitions_select_final',
            False,
            'error'
        )
        assert result == '0'

    def test_unknown_setting_with_error_action_raises(self):
        """Test that unknown settings raise an error when invalid_action is 'error'"""
        from clickhouse_connect.driver.exceptions import ProgrammingError

        with pytest.raises(ProgrammingError) as exc_info:
            self.validate_method(
                self.client,
                'unknown_setting_xyz',
                'some_value',
                'error'
            )
        assert 'unknown_setting_xyz' in str(exc_info.value)

    def test_unknown_setting_with_send_action_warns(self):
        """Test that unknown settings are sent with a warning when invalid_action is 'send'"""
        with patch('clickhouse_connect.driver.client.logger') as mock_logger:
            result = self.validate_method(
                self.client,
                'unknown_setting_xyz',
                'some_value',
                'send'
            )
            # Should return the string value (will be sent)
            assert result == 'some_value'
            # Should log a warning
            mock_logger.warning.assert_called_once()
            assert 'unknown_setting_xyz' in str(mock_logger.warning.call_args)

    def test_unknown_setting_with_drop_action_returns_none(self):
        """Test that unknown settings are dropped when invalid_action is 'drop'"""
        with patch('clickhouse_connect.driver.client.logger') as mock_logger:
            result = self.validate_method(
                self.client,
                'unknown_setting_xyz',
                'some_value',
                'drop'
            )
            # Should return None (will be dropped)
            assert result is None
            # Should log a warning
            mock_logger.warning.assert_called_once()
            assert 'unknown_setting_xyz' in str(mock_logger.warning.call_args)

    def test_readonly_setting_raises_error(self):
        """Test that readonly settings raise an error"""
        from clickhouse_connect.driver.exceptions import ProgrammingError

        # Make a setting readonly
        self.server_settings['max_threads'] = SettingDef(
            name='max_threads',
            value='0',
            readonly=1  # Readonly
        )

        with pytest.raises(ProgrammingError) as exc_info:
            self.validate_method(
                self.client,
                'max_threads',
                4,
                'error'
            )
        assert 'max_threads' in str(exc_info.value)
