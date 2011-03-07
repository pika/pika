# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


class PlainCredentials(object):
    """
    The PlainCredentials class returns the properly formatted username and
    password to the Connection. As of this version of Pika, only
    PlainCredentials are supported. To authenticate with Pika, simply create a
    credentials object passing in the username and password and pass that to
    the ConnectionParameters object.

    If you do not pass in credentials to the ConnectionParameters object, it
    will create credentials for 'guest' with the password of 'guest'.

    If you pass True to erase_on_connect the credentials will not be stored
    in memory after the Connection attempt has been made. This means that you
    will not be able to use a Reconnection Strategy successfully if this is
    enabled.
    """

    TYPE = 'PLAIN'

    def __init__(self, username, password, erase_on_connect=False):
        """
        Parameters:

        - username: plain text string value
        - password: plain text string value
        - erase_on_connect: bool erase credentials on connect. Default: False
        """
        self.username = username
        self.password = password
        self.erase_on_connect = erase_on_connect

    def response_for(self, start):
        """
        Validate that our type of authentication is supported
        """
        if PlainCredentials.TYPE not in start.mechanisms.split():
            return None, None

        return PlainCredentials.TYPE, '\0%s\0%s' % \
                                      (self.username, self.password)

    def erase_credentials(self):
        """
        Called by Connection when it no longer needs the credentials
        """
        if self.erase_on_connect:
            log.info("Erasing stored credential values")
            self.username = None
            self.password = None


# Append custom credential types to this list for validation support
VALID_TYPES = [PlainCredentials]
