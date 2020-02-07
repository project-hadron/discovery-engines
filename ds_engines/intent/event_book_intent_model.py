import inspect

from ds_engines.event_book.event_book import EventBook
from ds_foundation.intent.abstract_intent import AbstractIntentModel
from ds_foundation.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class EventBookIntentModel(AbstractIntentModel):

    def __init__(self, property_manager: AbstractPropertyManager, default_save_intent: bool=None):
        # set all the defaults
        default_save_intent = default_save_intent if isinstance(default_save_intent, bool) else True
        default_intent_level = 0
        intent_param_exclude = ['event_book']
        self.__running = False
        super().__init__(property_manager=property_manager, intent_param_exclude=intent_param_exclude,
                         default_save_intent=default_save_intent, default_intent_level=default_intent_level)

    def run_intent_pipeline(self, run_book: str, **kwargs) -> dict:
        """ Collectively runs all parameterised intent taken from the property manager against the code base as
        defined by the intent_contract.

        :param run_book: the event books to run containing the
        """
        book_portfolio = dict()
        for event_book in self._pm.get_run_book(book_name=run_book):
            intent_params = self._pm.get_intent(event_book)
            state_connector = intent_params.pop('state_connector', None)
            if isinstance(state_connector, str) and self._pm.has_connector(connector_name=state_connector):
                state_connector = self._pm.get_connector_contract(connector_name=state_connector)
            events_log_connector = intent_params.pop('events_log_connector', None)
            if isinstance(events_log_connector, str) and self._pm.has_connector(connector_name=events_log_connector):
                events_log_connector = self._pm.get_connector_contract(connector_name=events_log_connector)
            book_portfolio[event_book] = EventBook(book_name=event_book, intent_params=intent_params,
                                                   state_connector= state_connector,
                                                   events_log_connector=events_log_connector)
        return book_portfolio

    def set_event_book(self, book_name: str, time_distance: int=None, events_distance: int=None,
                       count_distance: int=None, state_connector: str=None, events_log_connector: str=None):
        """ auto categorises columns that have a max number of uniqueness with a min number of nulls
        and are object dtype

        :param book_name: The name of the event book for this intent
        :param time_distance: a time distance in seconds. Default to zero
        :param count_distance: a count distance. default to zero
        :param events_distance: an event counter distance for when to persist events
        :param state_connector: the connector name for the event book state
        :param events_log_connector: the connector name for the events log
        """
        # resolve intent persist options
        self._set_intend_signature(self._intent_builder(method=inspect.currentframe().f_code.co_name, params=locals()),
                                   intent_level=book_name, save_intent=True, replace=True)
        return


