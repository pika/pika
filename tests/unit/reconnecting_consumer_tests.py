"""
Tests for reconnecting consumer
"""

# Suppress pylint warnings concerning access to protected member
# pylint: disable=W0212

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

# Suppress pylint messages concerning invalid method name
# pylint: disable=C0103

from nose.tools import assert_equal, assert_false, assert_raises
from unittest.mock import MagicMock, patch
from examples.asynchronous_reconnecting_consumer import AsyncReconnectingConsumer


def mock_connection():
    """Fixture to mock the RabbitMQ connection."""
    connection = MagicMock()
    connection.is_closing = False
    connection.is_closed = False
    return connection


def create_consumer(mock_conn):
    """Create a consumer with a mocked connection."""
    with patch(
            "examples.asynchronous_reconnecting_consumer.pika.SelectConnection",
            return_value=mock_conn,
    ):
        amqp_url = "amqp://guest:guest@localhost:5672/%2F"
        return AsyncReconnectingConsumer(amqp_url)


def test_connect():
    """Test that the `connect` method sets up the connection correctly."""
    mock_conn = mock_connection()
    consumer = create_consumer(mock_conn)

    # Patch the __connect method to use the mock_connection
    with patch.object(consumer,
                      "_AsyncReconnectingConsumer__connect",
                      return_value=mock_conn):
        consumer.connect()

        # Verify that the connect_event was set and cleared
        assert_false(consumer.connect_event.is_set())

        # Verify that the connection was initialized correctly
        assert_equal(consumer._connection, mock_conn)


def test_reconnect():
    """Test the `reconnect` method behavior."""
    mock_conn = mock_connection()
    consumer = create_consumer(mock_conn)

    with patch.object(consumer, "stop") as mock_stop, patch.object(
            consumer, "connect") as mock_connect:
        consumer.reconnect()
        # Ensure `stop` and `connect` are called during reconnect
        mock_stop.assert_called_once()
        mock_connect.assert_called_once()


def test_on_connection_open():
    """Test the `on_connection_open` method behavior."""
    mock_conn = mock_connection()
    consumer = create_consumer(mock_conn)

    with patch.object(consumer, "open_channel") as mock_open_channel:
        consumer.on_connection_open(mock_conn)
        # Ensure the connect_event is cleared
        assert_false(consumer.connect_event.is_set())
        # Ensure the open_channel is called
        mock_open_channel.assert_called_once()


def test_reconnect_with_retries():
    """Test reconnect with a few failures before succeeding."""

    # Mock time.sleep to avoid actual delays
    with patch("time.sleep", return_value=None):

        # Simulate failures and success
        with patch.object(AsyncReconnectingConsumer, "connect") as mock_connect:

            # Configure mock to fail a few times and then succeed
            mock_connect.side_effect = [Exception("Simulated failure")] * 3 + [
                None
            ]

            # Create consumer instance
            amqp_url = "amqp://guest:guest@localhost:5672/%2F"
            consumer = AsyncReconnectingConsumer(amqp_url)

            # Call reconnect; expect it to eventually succeed
            consumer.reconnect(retry_count=5)

            # Verify connect was called the correct number of times
            assert_equal(mock_connect.call_count, 4)

            # Verify backoff time reset after success
            assert_equal(consumer._backoff_time, 1)
