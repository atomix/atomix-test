from logger import Logger
import uuid

_tests = {}

log = Logger('none', Logger.Type.TEST)

def test(name):
    """Registers a test function."""
    if callable(name):
        _tests[name.__name__] = name
    else:
        def wrap(f):
            _tests[name] = f
            return f
        return wrap

class Test(object):
    """Atomix test object."""
    def __init__(self, name):
        self.id = str(uuid.uuid4())
        self.name = name
        log.name = name

    def run(self):
        """Runs the test."""
        try:
            _tests[self.name]()
        except AssertionError, e:
            log.error(str(e))
            raise e
