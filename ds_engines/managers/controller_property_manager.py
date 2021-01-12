from aistac.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class ControllerPropertyManager(AbstractPropertyManager):

    DEFAULT_INTENT_LEVEL = 'primary_intent'

    def __init__(self, task_name: str, username: str):
        """Abstract Class for the Master Properties"""
        root_keys = []
        knowledge_keys = []
        super().__init__(task_name=task_name, root_keys=root_keys, knowledge_keys=knowledge_keys, username=username)

