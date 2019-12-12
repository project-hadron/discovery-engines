import shutil
import string
import unittest
import os
from datetime import datetime
from pprint import pprint

import pandas as pd

from ds_engines.engines.event_book import EventBook
from ds_behavioral import DataBuilderTools as tools


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
        event_book = EventBook.from_env()
        selection = list("ABCD")
        master = pd.DataFrame(columns=selection)
        event_book.add_event(event=master)
        self.assertEqual((0,4), event_book.current_state[1].shape)
        # add event
        event = pd.DataFrame({'A': [1,1,1], 'E': [1,1,1]})
        event_book.add_event(event=event)
        self.assertEqual((3, 5), event_book.current_state[1].shape)
        event = pd.DataFrame({'A': [1, 0, 1]})
        event_book.increment_event(event=event)
        control = pd.Series([2,1,2])
        result = event_book.current_state[1]['A']
        self.assertCountEqual(control, result)
        event_book.decrement_event(event=event)
        control = pd.Series([1,1,1])
        result = event_book.current_state[1]['A']
        self.assertCountEqual(control, result)

    def test_persist(self):
        engine = EventBook.from_env('primary_book')
        engine.set_persist_counters(state=5)
        engine.add_event(event=pd.DataFrame(columns=list("ABCD")))
        event = pd.DataFrame({'A': [1, 0, 1]})
        for _ in range(6):
            engine.increment_event(event=event)
        result = os.path.exists(os.path.join(os.environ['STATE_CONTRACT_PATH'], 'work'))
        self.assertEqual(True, result)


if __name__ == '__main__':
    unittest.main()
