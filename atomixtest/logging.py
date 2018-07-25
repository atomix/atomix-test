from __future__ import absolute_import
from logging import StreamHandler, DEBUG, getLogger, Formatter
from colorama import Fore, Back, init, Style

init()

class ColourStreamHandler(StreamHandler):

    """ A colorized output SteamHandler """

    # Some basic color scheme defaults
    colors = {
        'DEBUG': Fore.GREEN,
        'INFO': Fore.WHITE,
        'WARN': Fore.YELLOW,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRIT': Back.RED + Fore.WHITE,
        'CRITICAL': Back.RED + Fore.WHITE
    }

    def emit(self, record):
        try:
            record.msg = self.colors[record.levelname] + record.msg + Style.RESET_ALL
            self.stream.write(self.format(record))
            self.stream.write(getattr(self, 'terminator', '\n'))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def get_logger(name=None, fmt='%(asctime)-15s %(message)s'):
    """ Get and initialize a colourised logging instance if the system supports
    it as defined by the log.has_colour
    :param name: Name of the logger
    :type name: str
    :param fmt: Message format to use
    :type fmt: str
    :return: Logger instance
    :rtype: Logger
    """
    log = getLogger(name)
    # Only enable colour if support was loaded properly
    handler = ColourStreamHandler()
    handler.setLevel(DEBUG)
    handler.setFormatter(Formatter(fmt))
    log.addHandler(handler)
    log.setLevel(DEBUG)
    log.propagate = 0  # Don't bubble up to the root logger
    return log


_logger = None


def set_logger(name):
    """Sets the current logger."""
    global _logger
    _logger = get_logger(name)

def reset_logger():
    """Resets the logger to the default."""
    set_logger('test')
reset_logger()

class DynamicLogger(object):
    """Dynamic logger."""
    def __getattr__(self, name):
        return getattr(_logger, name)

logger = DynamicLogger()
