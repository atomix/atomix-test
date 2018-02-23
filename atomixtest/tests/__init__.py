from atomixtest.logger import log
import glob
import os
import inspect

def all_tests():
    tests = []
    for filename in glob.glob(os.path.dirname(__file__) + "/*.py"):
        modulefile = os.path.basename(filename)
        if modulefile != '__init__.py' and modulefile[-3:] == '.py':
            modulename = modulefile[:-3]
            module = __import__(modulename, globals(), locals())
            functions = inspect.getmembers(module)
            for funcname, func in functions:
                if funcname.startswith('test_'):
                    tests.append('{}.{}'.format(modulename, funcname))
    return tests

def get_test(name):
    modulename = '.'.join(name.split('.')[:-1])
    funcname = name.split('.')[-1]
    module = __import__(modulename, globals(), locals())
    return getattr(module, funcname)

def run_test(name):
    test = get_test(name)
    log.name = name
    try:
        test()
    except AssertionError, e:
        log.error(str(e))
        raise e
