import concurrent.futures
import os
import shutil
import unittest

import pandas as pd

from ds_engines.engines.event_book import EventBook
from tests.engines.state_model import StateModel


class MyTestCase(unittest.TestCase):

    def setUp(self):
        os.environ['STATE_CONTRACT_PATH'] = os.path.join(os.environ['PWD'], 'work')
        pass

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    def test_runs(self):
        state_engine = EventBook.from_env()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(StateModel, 1)
        StateModel().set_state_engine(event_book=state_engine)
        model = StateModel()
        model.state_engine().add_event(event=pd.DataFrame(columns=list("ABCD")))
        event = pd.DataFrame({'A': [1, 0, 1]})
        model.state_engine().add_event(event=event)
        result = StateModel().state_engine().current_state
        print(result[1].to_dict(orient='list'))




if __name__ == '__main__':
    unittest.main()
