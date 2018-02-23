
class TestError(Exception):
    """Base class for test errors."""


class UnknownClusterError(TestError):
    """Unknown cluster error."""
    def __init__(self, name):
        super(UnknownClusterError, self).__init__("Unknown cluster: %s" % (name,))
        self.name = name


class UnknownNodeError(TestError):
    """Unknown node error."""
    def __init__(self, name):
        super(UnknownNodeError, self).__init__("Unknown node: %s" % (name,))
        self.name = name


class UnknownNetworkError(TestError):
    """Unknown network error."""
    def __init__(self, name):
        super(UnknownNetworkError, self).__init__("Unknown network: %s" % (name,))
        self.name = name
