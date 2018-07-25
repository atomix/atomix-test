from imp import find_module, load_module
from inspect import isfunction
import os, sys, traceback
from logging import set_logger, reset_logger, logger
import time
from colorama import Fore, Back, init, Style

MODULE_EXTENSIONS = '.py'

_name = None

def _set_current_test(name):
    global _name
    _name = name
    return name

def get_current_test():
    return _name

def run(*paths):
    """Runs tests."""
    def find_functions(module, name=None):
        funcs = []
        if name is not None:
            funcs.append(module.__dict__[name])
        else:
            for func in module.__dict__.values():
                if isfunction(func) and func.__name__.startswith('test_'):
                    funcs.append(func)
        return funcs

    def scan_functions(module):
        funcs = []
        if hasattr(module, '__path__'):
            modulepath = module.__path__[0]
            for filename in os.listdir(modulepath):
                filepath = os.path.join(modulepath, filename)
                if not filename.startswith('__') and (os.path.isdir(filepath) or (os.path.isfile(filepath) and filename.endswith(MODULE_EXTENSIONS))):
                    modulename = filename[:-3]
                    file, pathname, description = find_module(modulename, module.__path__)
                    submodule = load_module(modulename, file, pathname, description)
                    funcs += scan_functions(submodule)
        funcs += find_functions(module)
        return funcs

    for path in paths:
        path_names = path.split('.')
        module, parent = None, None
        funcs = None
        for path_name in path_names:
            if module is not None and not hasattr(module, '__path__'):
                funcs = find_functions(module, path_name)
            else:
                parent = module.__path__ if module is not None else None
                file, pathname, description = find_module(path_name, parent)
                module = load_module(path_name, file, pathname, description)

        if funcs is None:
            funcs = scan_functions(module)

        for func in funcs:
            start = time.time()
            name = _set_current_test(func.__name__)
            text = "Running {}".format(name)
            print ''.join(['-' for i in range(len(text))])
            print text
            print ''.join(['-' for i in range(len(text))])
            try:
                set_logger(name)
                func()
            except:
                end = time.time()
                print Fore.RED + "{} failed in {} seconds".format(name, round(end - start, 4)) + Style.RESET_ALL
                traceback.print_exc(file=sys.stdout)
                return 1
            else:
                end = time.time()
                print Fore.GREEN + "{} passed in {} seconds".format(name, round(end - start, 4)) + Style.RESET_ALL
            finally:
                reset_logger()
                _set_current_test(None)
    return 0
