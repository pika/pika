import sys

class EventHandlerError(Exception): pass

class Event:
    def __init__(self, report_failures = False):
        self.handlers = {}
        self.report_failures = report_failures

    def addHandler(self, handler, key = None):
        if key is None:
            key = handler
        self.handlers[key] = handler

    def delHandler(self, key):
        self.handlers.pop(key, None)

    def fire(self, *args, **kwargs):
        results = {}
        errors = {}
        for key in self.handlers:
            try:
                results[key] = self.handlers[key](*args, **kwargs)
            except:
                errors[key] = sys.exc_info()

        if self.report_failures:
            return (results, errors)
        elif errors:
            raise EventHandlerError(errors)
        else:
            return results
