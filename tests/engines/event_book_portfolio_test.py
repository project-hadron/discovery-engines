import os
import shutil
import unittest

import pandas as pd

from ds_foundation.handlers.abstract_handlers import ConnectorContract

from ds_engines.components.event_book_portfolio import EventBookPortfolio
from ds_engines.managers.event_book_property_manager import EventBookPropertyManager


class EventBookPortfolioTest(unittest.TestCase):

    def setUp(self):
        os.environ['AISTAC_PM_PATH'] = os.path.join(os.environ['PWD'], 'work')
        pass

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    def test_runs(self):
        pm = EventBookPropertyManager('task')
        cc = ConnectorContract('connector', module_name='ds_foundation.handlers.dummy_handlers', handler='DummyPersistHandler')
        pm.set_property_connector(connector_contract=cc)
        EventBookPortfolio(property_manager=pm)

    def test_portfolio(self):
        engine = EventBookPortfolio.from_env('task')
        engine.add_event_book(book_name='book_one', start_book=True)
        self.assertEqual(['book_one'], engine.portfolio)
        engine.add_event_book(book_name='book_two', distance=5)
        self.assertEqual(['book_one'], engine.portfolio)
        print(engine.report_intent(stylise=False))
        engine.start_event_books()
        self.assertEqual(['book_one', 'book_two'], engine.portfolio)
        engine.remove_event_book(book_name='book_one')
        self.assertEqual(['book_two'], engine.portfolio)
        self.assertEqual(['book_two'], list(engine.pm.get_intent().get('0').keys()))

    def test_state_persist(self):
        engine = EventBookPortfolio.from_env('task')
        state_connector = ConnectorContract(uri=engine.pm.file_pattern('persist_book_state'),
                                            module_name=engine.PYTHON_MODULE_NAME,
                                            handler=engine.PYTHON_HANDLER)

        engine.add_event_book_connectors('persist_book_state', state_connector=state_connector)
        engine.add_event_book('persist_book', state_connector='persist_book_state', count_distance=1, start_book=True)
        engine.increment_event('persist_book', pd.DataFrame.from_dict(data={'A': [1,2,3], 'B': [3,4,5]}))


if __name__ == '__main__':
    unittest.main()
