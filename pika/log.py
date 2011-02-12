# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****
try:
    import curses
except ImportError:
    curses = None
from functools import wraps
import logging
import sys

logger = logging.getLogger('pika')
logger.setLevel(logging.WARN)

# Define these attributes as references to their logging counterparts
debug = logger.debug
error = logger.error
info = logger.info
warning = logger.warning

# Constants
DEBUG = logging.DEBUG
ERROR = logging.ERROR
INFO = logging.INFO
WARNING = logging.WARNING

# Default Level
LEVEL = INFO


def method_call(method):
    """
    Logging decorator to send the method and arguments to logger.debug
    """
    @wraps(method)
    def debug_log(*args, **kwargs):

        if logger.getEffectiveLevel() == DEBUG:

            # Get the class name of what was passed to us
            try:
                class_name = args[0].__class__.__name__
            except AttributeError:
                class_name = 'Unknown'
            except IndexError:
                class_name = 'Unknown'

            # Build a list of arguments to send to the logger
            log_args = list()
            for x in xrange(1, len(args)):
                log_args.append(args[x])
            if len(kwargs) > 1:
                log_args.append(kwargs)

            # If we have arguments, log them as well, otherwise just the method
            if log_args:
                logger.debug("%s.%s(%r) Called", class_name, method.__name__,
                             log_args)
            else:
                logger.debug("%s.%s() Called", class_name, method.__name__)

        # Actually execute the method
        return method(*args, **kwargs)

    # Return the debug_log function to the python stack for execution
    return debug_log


def setup(level=INFO, color=False):
    """
    Setup Pika logging, useful for console debugging and logging pika info,
    warning, and error messages.

    Parameters:

    - level: Logging level. One of DEBUG, ERROR, INFO, WARNING.
             Default: INFO
    - color: Use colorized output. Default: False
    """
    global curses

    if curses and not color:
        curses = None

    logging.basicConfig(level=level)
    logging.getLogger('pika').setLevel(level)

    if color and curses and sys.stderr.isatty():

        # Setup Curses
        curses.setupterm()

        # If we have color support
        if curses.tigetnum("colors") > 0:

            # Assign a FormatOutput Formatter to a StreamHandler instance
            for handler in logging.getLogger('').handlers:
                if isinstance(handler, logging.StreamHandler):
                    handler.setFormatter(FormatOutput())
                    break


class FormatOutput(logging.Formatter):
    """
    Creates a colorized output format for logging that helps provide easier
    context in debugging
    """
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        color = curses.tigetstr("setaf") or curses.tigetstr("setf") or ""
        self._level = {DEBUG: curses.tparm(color, 6),
                       ERROR: curses.tparm(color, 1),
                       INFO: curses.tparm(color, 2),
                       WARNING: curses.tparm(color, 3)}
        self._reset = curses.tigetstr("sgr0")
        self._class = curses.tparm(color, 4)
        self._args = curses.tparm(color, 2)
        elements = ['%(levelname)-8s', '%(asctime)-24s', '#%(process)s']
        self._prefix = '[%s]' % ' '.join(elements)

    def format(self, record):
        #timestamp = datetime.datetime.fromtimestamp(record.created)
        #record.timestamp = timestamp.isoformat(' ')

        message = record.getMessage()
        record.asctime = self.formatTime(record)
        if message[-6:] == 'Called':
            parts = message.split(' ')
            call = parts[0].split('.')
            end_method = call[1].find('(')
            start_content = message.find('(') + 1
            message = '%s%s.%s%s(%s%s%s)' % (self._class,
                                             call[0],
                                             call[1][:end_method],
                                             self._reset,
                                             self._args,
                                             message[start_content:-8],
                                             self._reset)

        output = "%s%s%s %s" % (self._level.get(record.levelno,
                                                self._reset),
                                self._prefix % record.__dict__,
                                self._reset,
                                message)
        if record.exc_info and not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            output = output.rstrip() + "\n    " + record.exc_text
        return output
