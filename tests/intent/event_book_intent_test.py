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
        im.set_event_book(book_name='book_two', count_distance=2, module_name='some.name', event_book_cls='MyEvents')
        result = self.pm.get_intent()
        control = {'0': {'book_one': {'kwargs': {}},
                         'book_two': {'module_name': 'some.name', 'event_book_cls': 'MyEvents', 'kwargs': {'count_distance': 2}}}}
        self.assertDictEqual(control, result)

    def test_run_intent_pipeline(self):
        im = EventBookIntentModel(self.pm)
        im.set_event_book(book_name='book_one')
        im.set_event_book(book_name='book_two', count_distance=2)
        result = im.run_intent_pipeline()
        self.assertCountEqual(['book_one', 'book_two'], result.keys())
        self.assertEqual('book_one', result.get('book_one').book_name)
        im.set_event_book(book_name='book_three', module_name='ds_engines.engines.event_books.pandas_event_book',
                          event_book_cls='PandasEventBook', intent_level=1)
        result = im.run_intent_pipeline(run_book=1)
        self.assertCountEqual(['book_three'], result.keys())
        self.assertEqual('book_three', result.get('book_three').book_name)



if __name__ == '__main__':
    unittest.main()
