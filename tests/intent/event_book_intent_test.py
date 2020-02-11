import unittest
import os
import shutil

from ds_foundation.handlers.abstract_handlers import ConnectorContract

from ds_engines.managers.event_book_property_manager import EventBookPropertyManager
from ds_engines.intent.event_book_intent_model import EventBookIntentModel
from ds_foundation.handlers.dummy_handlers import DummyPersistHandler


class EventBookIntentModelTest(unittest.TestCase):

    def setUp(self):
        os.environ['AISTAC_PM_PATH'] = os.path.join(os.environ['PWD'], 'work')
        self.pm = EventBookPropertyManager('task')
        cc = ConnectorContract('connector', module_name='ds_foundation.handlers.dummy_handlers', handler='DummyPersistHandler')
        self.pm.set_property_connector(connector_contract=cc)

    def tearDown(self):
        try:
            shutil.rmtree(os.path.join(os.environ['PWD'], 'work'))
        except:
            pass

    def test_runs(self):
        """Basic smoke test"""
        EventBookIntentModel(self.pm)

    def test_set_event_book(self):
        im = EventBookIntentModel(self.pm)
        im.set_event_book(book_name='book_one')
        im.set_event_book(book_name='book_two', count_distance=2)
        result = self.pm.get_intent()
        print(result)



if __name__ == '__main__':
    unittest.main()
