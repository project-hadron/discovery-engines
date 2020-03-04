import unittest
import os
import shutil

from aistac.properties.property_manager import PropertyManager

from ds_engines.managers.event_book_property_manager import EventBookPropertyManager


class EventBookPropertiesTest(unittest.TestCase):

    def setUp(self):
        os.environ['AISTAC_EB_URI'] = os.path.join(os.environ['PWD'], 'work')
        PropertyManager._remove_all()
        pass

    def tearDown(self):
        try:
            shutil.rmtree(os.path.join(os.environ['PWD'], 'work'))
        except:
            pass

    def test_runs(self):
        """Basic smoke test"""
        EventBookPropertyManager('test')



if __name__ == '__main__':
    unittest.main()
