import os
import shutil
import unittest

import pandas as pd

from aistac.handlers.abstract_handlers import ConnectorContract

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
        cc = ConnectorContract('connector', module_name='aistac.handlers.dummy_handlers', handler='DummyPersistHandler')
        pm.set_property_connector(connector_contract=cc)
        EventBookPortfolio(property_manager=pm)

    def test_portfolio(self):
        engine = EventBookPortfolio.from_env('task')
        engine.add_event_book(book_name='book_one', start_book=True)
        self.assertEqual(['book_one'], engine.active_books)
        engine.add_event_book(book_name='book_two', distance=5)
        self.assertEqual(['book_one'], engine.active_books)
        engine.start_event_books()
        self.assertEqual(['book_one', 'book_two'], engine.active_books)
        engine.remove_event_books(book_names='book_one')
        self.assertEqual(['book_two'], engine.active_books)
        self.assertEqual(['book_two'], list(engine.pm.get_intent().get('report_portfolio').keys()))


if __name__ == '__main__':
    unittest.main()
