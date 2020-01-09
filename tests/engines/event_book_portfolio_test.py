import concurrent.futures
import os
import shutil
import unittest

import pandas as pd

from ds_engines.engines.event_book import EventBook


class MyTestCase(unittest.TestCase):

    def setUp(self):
        os.environ['AISTAC_EB_URI'] = os.path.join(os.environ['PWD'], 'work')
        pass

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    def test_runs(self):
        pass




if __name__ == '__main__':
    unittest.main()
