from ds_foundation.handlers.abstract_handlers import ConnectorContract
from ds_foundation.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class EventBookPropertyManager(AbstractPropertyManager):
    """Class to deal with the properties of an event book"""
    MANAGER_NAME = 'state_engine'

    def __init__(self, contract_name):
        """ initialises the class specific to a state properties contract name

        :param contract_name: the contract reference name for these data properties
        """
        if contract_name is None or not isinstance(contract_name, str):
            assert ValueError("The contract_name can't be None or of zero length. '{}' passed".format(contract_name))
        keys = [{'distance': ['book', 'event']}, 'persist']
        super().__init__(manager=self.MANAGER_NAME, contract=contract_name, keys=keys)
        self._create_property_structure()

    @classmethod
    def from_properties(cls, contract_name: str, connector_contract: ConnectorContract, replace: bool=True):
        """ A Factory initialisation method to load the parameters from persistence at instantiation

        :param contract_name: the name of the contract or subset within the property manager
        :param connector_contract: the SourceContract bean for the SourcePersistHandler
        :param replace: (optional) if the loaded properties should replace any in memory
        """
        replace = replace if isinstance(replace, bool) else False
        instance = cls(contract_name=contract_name)
        instance.set_property_connector(resource=connector_contract.resource,
                                        connector_type=connector_contract.connector_type,
                                        location=connector_contract.location,
                                        module_name=connector_contract.module_name,
                                        handler=connector_contract.handler, **connector_contract.kwargs)
        if instance.get_connector_handler(instance.CONTRACT_CONNECTOR).exists():
            instance.load_properties(replace=replace)
        return instance

    def reset_contract_properties(self):
        """resets the data contract properties back to it's original state. It also resets the connector handler
        Note: this method ONLY writes to the properties memory and must be explicitly persisted
        using the ``save()'' method
        """
        super()._reset_abstract_properties()
        self._create_property_structure()
        return

    @property
    def event_distance(self) -> int:
        """Returns the events distance counter. Default to zero if not set"""
        return self.get(self.KEY.distance.event_key, 0)

    def set_event_distance(self, distance: int):
        """ sets the event distance. must be a positive integer. """
        distance = distance if isinstance(distance, int) and distance > 0 else 0
        self.set(self.KEY.distance.event_key, distance)
        return

    @property
    def book_distance(self) -> int:
        """Returns the book distance. Default to zero if not set"""
        return self.get(self.KEY.distance.book_key, 0)

    def set_book_distance(self, distance: int):
        """ sets the state evetns counter. must be a positive integer. """
        distance = distance if isinstance(distance, int) and distance > 0 else 0
        self.set(self.KEY.distance.book_key, distance)
        return

    @property
    def persist_intent(self) -> int:
        """Returns the persisted intent."""
        return self.get(self.KEY.persist_key, {})

    def set_persist_intent(self, persist_intent: dict):
        """ sets the persisted intent structure. """
        persist_intent = persist_intent if isinstance(persist_intent, dict) else {}
        self.set(self.KEY.persist_key, persist_intent)
        return

    def _create_property_structure(self):
        if not self.is_key(self.KEY.distance.book_key):
            self.set(self.KEY.distance.book_key, 0)
        if not self.is_key(self.KEY.distance.event_key):
            self.set(self.KEY.distance.event_key, 0)
        if not self.is_key(self.KEY.persist_key):
            self.set(self.KEY.persist_key, {})
        return
