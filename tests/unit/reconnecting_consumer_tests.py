"""
Tests for reconnecting consumer

"""

# Suppress pylint warnings concerning access to protected member
# pylint: disable=W0212

# Suppress pylint messages concerning missing docstrings
# pylint: disable=C0111

# Suppress pylint messages concerning invalid method name
# pylint: disable=C0103

import pytest
from unittest.mock import MagicMock, patch
from asyncio import Event
from examples.asynchronous_reconnecting_consumer import AsyncReconnectingConsumer


@pytest.fixture
def mock_connection():
    """Fixture to mock the RabbitMQ connection."""
    connection = MagicMock()
    connection.is_closing = False
    connection.is_closed = False
    return connection


@pytest.fixture
def consumer(mock_connection):
    """Fixture to create a consumer with a mocked connection."""
    with patch(
        "examples.asynchronous_reconnecting_consumer.pika.SelectConnection",
        return_value=mock_connection,
    ):
        amqp_url = "amqp://guest:guest@localhost:5672/%2F"
        return AsyncReconnectingConsumer(amqp_url)


def test_connect(consumer, mock_connection):
    """Test that the `connect` method sets up the connection correctly."""
    # Patch the __connect method to use the mock_connection
    with patch.object(
        consumer, "_AsyncReconnectingConsumer__connect", return_value=mock_connection
    ):
        consumer.connect()

        # Verify that the connect_event was set and cleared
        assert consumer.connect_event.is_set() is False

        # Verify that the connection was initialized correctly
        assert consumer._connection is mock_connection


def test_reconnect(consumer, mock_connection):
    """Test the `reconnect` method behavior."""
    with patch.object(consumer, "stop") as mock_stop, patch.object(
        consumer, "connect"
    ) as mock_connect:
        consumer.reconnect()
        # Ensure `stop` and `connect` are called during reconnect
        mock_stop.assert_called_once()
        mock_connect.assert_called_once()


def test_on_connection_open(consumer, mock_connection):
    """Test the `on_connection_open` method behavior."""
    with patch.object(consumer, "open_channel") as mock_open_channel:
        consumer.on_connection_open(mock_connection)
        # Ensure the connect_event is cleared
        assert consumer.connect_event.is_set() is False
        # Ensure the open_channel is called
        mock_open_channel.assert_called_once()


def test_reconnect_with_retries():
    """Test reconnect with a few failures before succeeding."""

    # Mock time.sleep to avoid actual delays
    with patch("time.sleep", return_value=None):

        # simulate failures and success
        with patch.object(AsyncReconnectingConsumer, "connect") as mock_connect:

            # Configure mock to fail a few times and then succeed
            mock_connect.side_effect = [Exception("Simulated failure")] * 3 + [None]

            # Create consumer instance
            amqp_url = "amqp://guest:guest@localhost:5672/%2F"
            consumer = AsyncReconnectingConsumer(amqp_url)

            # Call reconnect; expect it to eventually succeed
            consumer.reconnect(retry_count=5)

            # connect was called the correct number of times
            assert mock_connect.call_count == 4

            # backoff time reset after success
            assert consumer._backoff_time == 1
