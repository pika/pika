import sys

class Event:
    def __init__(self):
        self.handlers = {}

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
        return (results, errors)
