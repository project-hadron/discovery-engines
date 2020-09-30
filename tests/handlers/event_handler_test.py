import unittest
import os
import shutil

from aistac.handlers.abstract_handlers import ConnectorContract
from ds_behavioral import SyntheticBuilder
from ds_behavioral.intent.synthetic_intent_model import SyntheticIntentModel
from aistac.properties.property_manager import PropertyManager

from ds_engines import EventBookPortfolio
from ds_engines.handlers.event_handlers import EventSourceHandler, EventPersistHandler


class EventHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        # clean out any old environments
        for key in os.environ.keys():
            if key.startswith('HADRON'):
                del os.environ[key]

        os.environ['HADRON_PM_PATH'] = os.path.join('work', 'config')
        os.environ['HADRON_DEFAULT_PATH'] = os.path.join('work', 'data')
        try:
            os.makedirs(os.environ['HADRON_PM_PATH'])
            os.makedirs(os.environ['HADRON_DEFAULT_PATH'])
        except:
            pass
        PropertyManager._remove_all()

    def tearDown(self):
        try:
            shutil.rmtree('work')
        except:
            pass

    @property
    def tools(self) -> SyntheticIntentModel:
        return SyntheticBuilder.scratch_pad()

    def test_handler(self):
        """Basic smoke test"""
        # set up an EventBook Portfolio
        ebp: EventBookPortfolio = EventBookPortfolio.from_env(task_name='test_portfolio', has_contract=False, default_save=False)
        ebp.intent_model.add_event_book('test_book')
        # test the handler
        cc = ConnectorContract(uri='ebp://test_portfolio/test_book', module_name='', handler='')
        handler = EventSourceHandler(connector_contract=cc)

    def test_raise(self):
        with self.assertRaises(KeyError) as context:
            env = os.environ['NoEnvValueTest']
        self.assertTrue("'NoEnvValueTest'" in str(context.exception))


if __name__ == '__main__':
    unittest.main()
