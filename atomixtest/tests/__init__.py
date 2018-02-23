import glob
import os

def _find_tests():
    test_modules = []
    for name in glob.glob(os.path.dirname(__file__) + "/*.py"):
        module_file = os.path.basename(name)
        if module_file != '__init__.py' and module_file[-3:] == '.py':
            test_modules.append(module_file[:-3])
    return test_modules

__all__ = _find_tests()
