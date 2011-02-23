# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

import pika.log as log


class PlainCredentials(object):
    """
    The PlainCredentials class returns the properly formatted username and
    password to the Connection. As of this version of Pika, only
    PlainCredentials are supported. To authenticate with Pika, simply create a
    credentials object passing in the username and password and pass that to
    the ConnectionParameters object.

    If you do not pass in credentials to the ConnectionParameters object, it
    will create credentials for 'guest' with the password of 'guest'.
    """

    TYPE = 'PLAIN'

    @log.method_call
    def __init__(self, username, password):
        """
        Parameters:

        - username: plain text string value

        - password: plain text string value
        """
        self.username = username
        self.password = password

    @log.method_call
    def response_for(self, start):
        if PlainCredentials.TYPE not in start.mechanisms.split():
            return None

        return PlainCredentials.TYPE, '\0%s\0%s' % \
                                      (self.username, self.password)
