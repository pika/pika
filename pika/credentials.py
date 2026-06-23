"""
The credentials classes are used to encapsulate all authentication information for the
:class:`~pika.connection.ConnectionParameters` class.

The :class:`~pika.credentials.PlainCredentials` class returns the properly formatted username and
password to the :class:`~pika.connection.Connection`.

To authenticate with Pika, create a :class:`~pika.credentials.PlainCredentials` object passing in
the username and password and pass it as the credentials argument value to the
:class:`~pika.connection.ConnectionParameters` object.

If you are using :class:`~pika.connection.URLParameters` you do not need a credentials object, one
will automatically be created for you.

If you are looking to implement SSL certificate style authentication, you would extend the
:class:`~pika.credentials.ExternalCredentials` class implementing the required behavior.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Type, Union

from ._utils import as_bytes

if TYPE_CHECKING:
    from pika.spec import Connection

LOGGER = logging.getLogger(__name__)


class PlainCredentials:
    """
    A credentials object for the default authentication methodology with RabbitMQ.

    If you do not pass in credentials to the ConnectionParameters object, it will create credentials
    for 'guest' with the password of 'guest'.

    If you pass True to erase_on_connect the credentials will not be stored in memory after the
    Connection attempt has been made.

    :param username: The username to authenticate with
    :param password: The password to authenticate with
    :param erase_on_connect: erase credentials on connect.
    """

    TYPE = 'PLAIN'

    def __init__(self,
                 username: str,
                 password: str,
                 erase_on_connect: bool = False) -> None:
        """
        Create a new instance of PlainCredentials.

        :param username: The username to authenticate with
        :param password: The password to authenticate with
        :param erase_on_connect: erase credentials on connect.
        """
        self.username: str | None = username
        self.password: str | None = password
        self.erase_on_connect: bool = erase_on_connect

    def __eq__(self, other) -> bool:
        if isinstance(other, PlainCredentials):
            return (self.username == other.username and
                    self.password == other.password and
                    self.erase_on_connect == other.erase_on_connect)
        return NotImplemented

    def __ne__(self, other) -> bool:
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    def response_for(
            self, start: Connection.Start) -> tuple[str | None, bytes | None]:
        """
        Validate that this type of authentication is supported.

        :param start: Connection.Start method
        """
        if as_bytes(PlainCredentials.TYPE) not in as_bytes(
                start.mechanisms).split():
            return None, None
        return (PlainCredentials.TYPE, b'\0' + as_bytes(self.username or '') +
                b'\0' + as_bytes(self.password or ''))

    def erase_credentials(self) -> None:
        """Called by Connection when it no longer needs the credentials."""
        if self.erase_on_connect:
            LOGGER.info("Erasing stored credential values")
            self.username = None
            self.password = None


class ExternalCredentials:
    """The ExternalCredentials class allows the connection to use EXTERNAL authentication, generally
    with a client SSL certificate.
    """

    TYPE = 'EXTERNAL'

    def __init__(self) -> None:
        """Create a new instance of ExternalCredentials."""
        self.erase_on_connect = False

    def __eq__(self, other) -> bool:
        if isinstance(other, ExternalCredentials):
            return self.erase_on_connect == other.erase_on_connect
        return NotImplemented

    def __ne__(self, other) -> bool:
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    def response_for(
            self, start: Connection.Start) -> tuple[str | None, bytes | None]:
        """
        Validate that this type of authentication is supported.

        :param start: Connection.Start method
        """
        if as_bytes(ExternalCredentials.TYPE) not in as_bytes(
                start.mechanisms).split():
            return None, None
        return ExternalCredentials.TYPE, b''

    def erase_credentials(self) -> None:
        """Called by Connection when it no longer needs the credentials."""
        LOGGER.debug('Not supported by this Credentials type')


_CredentialType = Union[Type[PlainCredentials], Type[ExternalCredentials]]

# Append custom credential types to this list for validation support
VALID_TYPES: list[_CredentialType] = [PlainCredentials, ExternalCredentials]
