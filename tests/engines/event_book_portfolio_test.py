import os
import shutil
import unittest

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
        engine.update_portfolio()
        self.assertEqual(['book_one', 'book_two'], engine.portfolio)
        engine.remove_event_book(book_name='book_one')
        self.assertEqual(['book_two'], engine.portfolio)
        self.assertEqual(['book_two'], list(engine.pm.get_intent().get('0').keys()))


if __name__ == '__main__':
    unittest.main()
