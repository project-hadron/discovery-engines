from ds_foundation.handlers.abstract_handlers import ConnectorContract
from ds_foundation.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class EventBookPropertyManager(AbstractPropertyManager):
    """Class to deal with the properties of an event book"""

    def __init__(self, task_name: str):
        # set additional keys
        root_keys = []
        knowledge_keys = []
        super().__init__(task_name=task_name, root_keys=root_keys, knowledge_keys=knowledge_keys)