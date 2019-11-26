import shutil
import string
import unittest
import warnings
import os
from datetime import datetime
from pprint import pprint

import matplotlib
import numpy as np
import pandas as pd

from ds_engines.engines.state_engine import StateEngine
from ds_behavioral import DataBuilderTools as tools

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


class StateEngineTest(unittest.TestCase):

    def setUp(self):
        os.environ['STATE_CONTRACT_PATH'] = os.path.join(os.environ['PWD'], 'work', 'contract')
        pass

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    def test_runs(self):
        """Basic smoke test"""
        StateEngine.from_env()

    def test_events(self):
        engine = StateEngine.from_env()
        selection = list(string.ascii_uppercase)
        master = pd.DataFrame(columns=selection)
        engine.add_event(event=master)
        for counter in range(100):
            sample = tools.get_number(1, 10).pop()
            # create the event DataFrame with a randome number of headers
            columns = tools.get_category(selection=selection, at_most=1, size=sample)
            event = pd.DataFrame(columns=columns)
            # random bulk upload
            ids = tools.get_number(100, 999, at_most=1, size=tools.get_number(1, 10))
            for id in ids:
                values = tools.get_number(100, size=sample)
                event.loc[id] = values
            # add the event
            engine.add_event(event=event)
        _, result = engine.get_current_state()
        engine.persist_state(result)
        print(engine)

    def test_speed(self):
        engine = StateEngine.from_env()
        selection = list(string.ascii_uppercase)
        master = pd.DataFrame(columns=selection)
        engine.add_event(event=master)
        event = pd.DataFrame(columns=list('ABC'))
        event.loc['cu_1'] = [1,1,1]
        start = datetime.now()
        for counter in range(1000):
            engine.add_event(event=event)
        end = datetime.now()
        print("It took {}".format(end - start))

if __name__ == '__main__':
    unittest.main()
