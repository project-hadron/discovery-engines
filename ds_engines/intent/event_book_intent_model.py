import inspect

from aistac.handlers.abstract_event_book import EventBookContract, EventBookFactory
from ds_engines.engines.event_books.pandas_event_book import PandasEventBook
from aistac.intent.abstract_intent import AbstractIntentModel
from aistac.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class EventBookIntentModel(AbstractIntentModel):

    _PORTFOLIO_LEVEL = 'report_portfolio'

    def __init__(self, property_manager: AbstractPropertyManager, default_save_intent: bool=None,
                 intent_type_additions: list=None):
        """initialisation of the Intent class.

        :param property_manager: the property manager class that references the intent contract.
        :param default_save_intent: (optional) The default action for saving intent in the property manager
        :param intent_type_additions: (optional) if additional data types need to be supported as an intent param
        """
        # set all the defaults
        default_save_intent = default_save_intent if isinstance(default_save_intent, bool) else True
        intent_type_additions = intent_type_additions if isinstance(intent_type_additions, list) else list()
        default_replace_intent = True
        default_intent_level = 'primary'
        intent_param_exclude = ['start_book', 'book_name']
        super().__init__(property_manager=property_manager, intent_param_exclude=intent_param_exclude,
                         default_save_intent=default_save_intent, default_intent_level=default_intent_level,
                         default_replace_intent=default_replace_intent, intent_type_additions=intent_type_additions)

    def run_intent_pipeline(self, exclude_books: [str, list]=None, **kwargs):
        """ Collectively runs all parameterised intent taken from the property manager against the code base as
        defined by the intent_contract.

        :param exclude_books: (optional) a list of book_names in the report_portfolio not to start
        """
        exclude_books = self._pm.list_formatter(exclude_books)
        book_portfolio = dict()
        if self._pm.has_intent():
            for book_name, params in self._pm.get_intent(level=self._PORTFOLIO_LEVEL).items():
                if book_name in exclude_books:
                    continue
                params.update(params.pop('kwargs', {}))
                eb = eval(f"self.set_event_book(book_name='{book_name}', start_book=True, "
                          f"save_intent=False, **{params})")
                book_portfolio.update({book_name: eb})
        return book_portfolio

    def set_event_book(self, book_name: str, module_name: str=None, event_book_cls: str=None, start_book: bool=None,
                       save_intent: bool=None, **kwargs):
        """ creates an event book and/or saves the event book intent

        :param book_name: the reference book name
        :param module_name: (optional) if passing connectors, The module name where the Event Book class can be found
        :param event_book_cls: (optional) if passing connectors. The name of the Event Book class to instantiate
        :param start_book: (optional) if the event book should be created and returned.
        :param save_intent: (optional) save the intent to the Intent Properties. defaults to the default_save_intent
        """
        # resolve intent persist options
        replace_intent = True
        intent_params = self._intent_builder(method=inspect.currentframe().f_code.co_name, params=locals())
        intent_params.update({book_name: intent_params.pop(inspect.currentframe().f_code.co_name)})
        self._set_intend_signature(intent_params, intent_level=self._PORTFOLIO_LEVEL, save_intent=save_intent,
                                   replace_intent=replace_intent)
        # create the event book
        if isinstance(start_book, bool) and start_book:
            if not isinstance(module_name, str) or not isinstance(event_book_cls, str):
                state_connector = kwargs.pop('state_connector', None)
                if isinstance(state_connector, str) and self._pm.has_connector(connector_name=state_connector):
                    state_connector = self._pm.get_connector_contract(connector_name=state_connector)
                events_log_connector = kwargs.pop('events_log_connector', None)
                if self._pm.has_connector(connector_name=events_log_connector):
                    events_log_connector = self._pm.get_connector_contract(connector_name=events_log_connector)
                time_distance = kwargs.pop('time_distance', 0)
                count_distance = kwargs.pop('count_distance', 0)
                events_log_distance = kwargs.pop('events_log_distance', 0)
                return PandasEventBook(book_name=book_name, time_distance=time_distance, count_distance=count_distance,
                                       events_log_distance=events_log_distance, state_connector=state_connector,
                                       events_log_connector=events_log_connector)
            else:
                event_book_contract = EventBookContract(book_name=book_name, module_name=module_name,
                                                        event_book_cls=event_book_cls, **kwargs)
                return EventBookFactory.instantiate(event_book_contract=event_book_contract)
        return
