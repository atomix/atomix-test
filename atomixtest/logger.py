from __future__ import print_function
from __future__ import unicode_literals
from colorama import Fore, Back, Style

class Logger(object):
    """Logger object."""
    class Type(object):
        """Logger type."""
        FRAMEWORK = ((Back.RESET, Fore.WHITE), (Back.RESET, Fore.CYAN))
        CLIENT = ((Back.CYAN, Fore.WHITE), (Back.RESET, Fore.MAGENTA))
        TEST = ((Back.GREEN, Fore.WHITE), (Back.RESET, Fore.GREEN))
        ERROR = ((Back.RED, Fore.WHITE), (Style.RESET_ALL,))

    def __init__(self, name, type):
        self.name = name
        self.type = type

    def _output(self, type, message, *args, **kwargs):
        prefix = ''.join(type[0]) + self.name
        suffix = ''.join(type[1]) + ' ' + message.format(*args, **kwargs)
        print(prefix + suffix)

    def message(self, message, *args, **kwargs):
        self._output(self.type, message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self._output(Logger.Type.ERROR, message, *args, **kwargs)

log = Logger('test', Logger.Type.TEST)
