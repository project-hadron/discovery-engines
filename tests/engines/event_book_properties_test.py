import unittest
import os
import shutil

import warnings

import matplotlib
matplotlib.use("TkAgg")


def ignore_warnings(message: str = None):
    def real_ignore_warnings(func):
        def do_test(self, *args, **kwargs):
            with warnings.catch_warnings():
                if isinstance(message, str) and len(message) > 0:
                    warnings.filterwarnings("ignore", message=message)
                else:
                    warnings.simplefilter("ignore")
                func(self, *args, **kwargs)

        return do_test

    return real_ignore_warnings


class EventBookPropertiesTest(unittest.TestCase):

    def setUp(self):
        os.environ['CONTRACT_PATH'] = os.path.join(os.environ['PWD'], 'work')
        pass

    def tearDown(self):
        try:
            shutil.rmtree(os.path.join(os.environ['PWD'], 'work'))
        except:
            pass

    def test_runs(self):
        """Basic smoke test"""
        pass

    @ignore_warnings
    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
