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

from ds_engines.engines.event_book import EventBook
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
        os.environ['STATE_CONTRACT_PATH'] = os.path.join(os.environ['PWD'], 'work')
        pass

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    def test_runs(self):
        """Basic smoke test"""
        EventBook.from_env()

    def test_events(self):
        engine = EventBook.from_env()
        selection = list("ABCD")
        master = pd.DataFrame(columns=selection)
        engine.add_event(event=master)
        self.assertEqual((0,4), engine.current_state[1].shape)
        # add event
        event = pd.DataFrame({'A': [1,1,1], 'E': [1,1,1]})
        engine.add_event(event=event)
        self.assertEqual((3, 5), engine.current_state[1].shape)
        event = pd.DataFrame({'A': [1, 0, 1]})
        engine.increment_event(event=event)
        control = pd.Series([2,1,2])
        result = engine.current_state[1]['A']
        self.assertCountEqual(control, result)
        engine.decrement_event(event=event)
        control = pd.Series([1,1,1])
        result = engine.current_state[1]['A']
        self.assertCountEqual(control, result)

    def test_speed(self):
        engine = EventBook.from_env()
        selection = list(string.ascii_uppercase)
        master = pd.DataFrame(columns=selection)
        engine.increment_event(event=master)
        event = pd.DataFrame(columns=list('ABC'))
        event.loc['cu_1'] = [1,1,1]
        start = datetime.now()
        for counter in range(100):
            engine.add_event(event=event)
        end = datetime.now()
        print("It took {}".format(end - start))

    def test_persist(self):
        engine = EventBook.from_env()
        engine.set_persist_counters(state=5)
        engine.add_event(event=pd.DataFrame(columns=list("ABCD")))
        event = pd.DataFrame({'A': [1, 0, 1]})
        for _ in range(6):
            engine.increment_event(event=event)
        result = os.path.exists(os.path.join(os.environ['STATE_CONTRACT_PATH'], 'work'))


if __name__ == '__main__':
    unittest.main()
