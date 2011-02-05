Log
===
pika.log is a wrapper which provides a wrapper for the `python logging <http://docs.python.org/library/logging.html>`_ module. If the pika.log.DEBUG is set to true, it will enable pika to send its debugging information to logging.debug. We use this to make sure that pika does not impact applications who use logging.debug by not sending Pika's internal debug information by default.

To turn Pika debug logging on::

     import logging
     import pika.log

     pika.log.DEBUG = True

     logging.basicConfig(level=logging.DEBUG)

Once this is done you will receive all low level information about what Pika is doing including the frames it is sending and receiving from RabbitMQ.

.. function:: debug(msg[, *args[, **kwargs]])

   Logs a message with level :const:`DEBUG` on the root logger. The *msg* is the
   message format string, and the *args* are the arguments which are merged into
   *msg* using the string formatting operator. (Note that this means that you can
   use keywords in the format string, together with a single dictionary argument.)

   There are two keyword arguments in *kwargs* which are inspected: *exc_info*
   which, if it does not evaluate as false, causes exception information to be
   added to the logging message. If an exception tuple (in the format returned by
   :func:`sys.exc_info`) is provided, it is used; otherwise, :func:`sys.exc_info`
   is called to get the exception information.

   The other optional keyword argument is *extra* which can be used to pass a
   dictionary which is used to populate the __dict__ of the LogRecord created for
   the logging event with user-defined attributes. These custom attributes can then
   be used as you like. For example, they could be incorporated into logged
   messages. For example::

      FORMAT = "%(asctime)-15s %(clientip)s %(user)-8s %(message)s"
      logging.basicConfig(format=FORMAT)
      d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}
      logging.warning("Protocol problem: %s", "connection reset", extra=d)

   would print something like::

      2006-02-08 22:20:02,165 192.168.0.1 fbloggs  Protocol problem: connection reset

   The keys in the dictionary passed in *extra* should not clash with the keys used
   by the logging system. (See the :class:`Formatter` documentation for more
   information on which keys are used by the logging system.)

   If you choose to use these attributes in logged messages, you need to exercise
   some care. In the above example, for instance, the :class:`Formatter` has been
   set up with a format string which expects 'clientip' and 'user' in the attribute
   dictionary of the LogRecord. If these are missing, the message will not be
   logged because a string formatting exception will occur. So in this case, you
   always need to pass the *extra* dictionary with these keys.

   While this might be annoying, this feature is intended for use in specialized
   circumstances, such as multi-threaded servers where the same code executes in
   many contexts, and interesting conditions which arise are dependent on this
   context (such as remote client IP address and authenticated user name, in the
   above example). In such circumstances, it is likely that specialized
   :class:`Formatter`\ s would be used with particular :class:`Handler`\ s.

   .. versionchanged:: 2.5
      *extra* was added.

