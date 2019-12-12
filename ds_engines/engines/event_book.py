import os
import threading
from datetime import datetime
import pandas as pd

from ds_engines.managers.event_book_properties import EventBookPropertyManager
from ds_foundation.handlers.abstract_handlers import ConnectorContract
from ds_foundation.properties.decorator_patterns import singleton

__author__ = 'Darryl Oatridge'


class EventBook(object):

    CONNECTOR_BOOK = 'book_connector'
    CONNECTOR_EVENTS = 'events_connector'
    CONNECTOR_PROPERTIES: str

    __book_state: pd.DataFrame
    __events_log: dict
    __event_count: int
    __book_count: int

    def __init__(self, book_name: str, event_book_properties: [ConnectorContract]):
        """ Encapsulation class for the management of Event Books

        :param book_name: The name of the event book
        :param event_book_properties: The persist handler for the event book properties
        """
        if not isinstance(book_name, str) or len(book_name) < 1:
            raise ValueError("The contract name must be a valid string")
        self._book_name = book_name
        # set property managers
        self._book_pm = EventBookPropertyManager.from_properties(book_name=self._book_name,
                                                                 connector_contract=event_book_properties)
        self.CONNECTOR_PROPERTIES = self._book_pm.CONTRACT_CONNECTOR
        if self._book_pm.has_persisted_properties():
            self._book_pm.load_properties()
        # initialise the values
        self._book_pm.persist_properties()
        # initialise the globals
        self.__book_state = pd.DataFrame()
        self.__events_log = dict()
        self.__event_count = 0
        self.__book_count = 0

    @classmethod
    def from_remote(cls, book_name: str, location: str=None, path: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers from the default remote.
        This assumes the use of the pandas handler module and pickle persistence on a remote default.

         :param book_name: (optional) The reference name of the event book. Default 'primary_book'
         :param location: (optional) the bucket where the data resource can be found. Default 'ds_discovery'
         :param path: (optional) the path to the persist resources. default 'event_book/persist/{book_name}/'
         :return: the initialised class instance
         """
        _location = 'ds-discovery' if not isinstance(location, str) else location
        _path = os.path.join('persist', 'event_book', book_name) if not isinstance(path, str) else path
        _module_name = 'ds_connectors.handlers.aws_s3_handlers'
        _handler_name = 'AwsS3PersistHandler'
        _properties_resource = os.path.join(_path, 'config_event_book_{}.pickle'.format(book_name))
        _properties_connector = ConnectorContract(resource=_properties_resource, connector_type='pickle',
                                                  location=_location, module_name=_module_name,
                                                  handler='AwsS3PersistHandler')
        rtn_cls = cls(book_name=book_name, event_book_properties=_properties_connector)
        if not rtn_cls.book_pm.has_connector(cls.CONNECTOR_BOOK):
            rtn_cls.set_state_persist_contract(path=_path, location=_location, module_name=_module_name,
                                               handler=_handler_name, **kwargs)
        if not rtn_cls.book_pm.has_connector(cls.CONNECTOR_EVENTS):
            rtn_cls.set_events_persist_contract(path=_path, location=_location, module_name=_module_name,
                                                handler=_handler_name, **kwargs)

        return rtn_cls

    @classmethod
    def from_path(cls, book_name: str=None, path: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers from the data paths.
        This assumes the use of the pandas handler module.

        :param path: the path persist path
        :param book_name: (optional) The reference name of the event book. Default 'primary_book'
        :return: the initialised class instance
        """
        _book_name = book_name if isinstance(book_name, str) else 'primary_book'
        _path = os.path.join(os.getcwd(), 'persist', _book_name) if not isinstance(path, str) else path
        _module_name = 'ds_discovery.handlers.pandas_handlers'
        _handler_name = 'PandasPersistHandler'
        _location = os.path.join(_path, _book_name)
        _properties_connector = ConnectorContract(resource="config_event_book_{}.yaml".format(_book_name),
                                                  connector_type='yaml', location=_location, module_name=_module_name,
                                                  handler=_handler_name)
        rtn_cls = cls(book_name=_book_name, event_book_properties=_properties_connector)
        if not rtn_cls.book_pm.has_connector(cls.CONNECTOR_BOOK):
            rtn_cls.set_state_persist_contract(path=_path, location=_location, module_name=_module_name,
                                               handler=_handler_name, **kwargs)
        if not rtn_cls.book_pm.has_connector(cls.CONNECTOR_EVENTS):
            rtn_cls.set_events_persist_contract(path=_path, location=_location, module_name=_module_name,
                                                handler=_handler_name, **kwargs)
        return rtn_cls

    @classmethod
    def from_env(cls, book_name: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers taking the property contract path from
        the os.environ['EVENT_BOOK_PATH'] or locally from the current working directory 'event_book/contracts' if
        no environment variable is found. This assumes the use of the pandas handler module and yaml persisted file.

         :param book_name: The reference name of the event book
         :return: the initialised class instance
         """
        _path = os.environ['EVENT_BOOK_PATH'] if 'EVENT_BOOK_PATH' in os.environ.keys() else None
        return cls.from_path(book_name=book_name, path=_path, **kwargs)

    @property
    def book_name(self) -> str:
        """The contract name of this transition instance"""
        return self._book_name

    @property
    def version(self) -> str:
        """The version number of the contracts"""
        return self.book_pm.version

    @property
    def book_pm(self) -> EventBookPropertyManager:
        """The data properties manager instance"""
        if self._book_pm is None or self._book_pm.contract_name != self.book_name:
            self._book_pm = EventBookPropertyManager(self._book_name)
        return self._book_pm

    @property
    def current_state(self) -> (datetime, pd.DataFrame):
        return datetime.now(), self.__book_state.copy(deep=True)

    def add_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['add', event]})
        self.__book_state = event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def increment_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['increment', event]})
        _event = event.combine(self.__book_state, lambda s1, s2: s2 + s1 if len(s2.mode()) else s1)
        self.__book_state = _event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def decrement_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['decrement', event]})
        _event = event.combine(self.__book_state, lambda s1, s2: s2 - s1 if len(s2.mode()) else s1)
        self.__book_state = _event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def _update_counters(self):
        self.__book_count += 1
        self.__event_count += 1
        if 0 < self.book_pm.book_distance <= self.__book_count:
            self.__book_count = 0
            self.persist_book()
            self.__events_log = dict()
            self._persist_events()
        if 0 < self.book_pm.event_distance <= self.__event_count:
            self.__event_count = 0
            self._persist_events()
        return

    def persist_book(self, stamped: bool=False):
        """Saves the book to persistence"""
        stamped = False if not isinstance(stamped, bool) else stamped
        if self.book_pm.has_connector(self.CONNECTOR_BOOK):
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_BOOK)
            if stamped:
                self.set_state_persist_contract()

            handler.persist_canonical(self.current_state[1])
        return

    def _persist_events(self):
        """Saves the pandas.DataFrame to the persisted stater"""
        if self.book_pm.has_connector(self.CONNECTOR_EVENTS):
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_EVENTS)
            handler.persist_canonical(self.__events_log)
        return

    def generate_resource_name(self, label: str) -> (str, str):
        """ Returns a persist pattern based on time, contract name, the book and version"""
        _pattern = "{}_{}_{}_{}.pickle"
        _time = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return _pattern.format(str(datetime.now()), self.book_name, label, self.version), 'pickle'

    def set_state_persist_contract(self, path: str=None, location: str=None, module_name: str=None,
                                   handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param path: (optional) the path to the persist resources. default 'persist/{book_name}/'
        :param location: (optional) a path, region or uri reference that can be used to identify location of resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        return self._set_persist_contract(connector_name=self.CONNECTOR_BOOK, path=path, location=location,
                                          module_name=module_name, handler=handler, **kwargs)

    def set_events_persist_contract(self, path: str=None, location: str=None, module_name: str=None,
                                    handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param path: (optional) the path to the persist resources. default 'persist/{book_name}/'
        :param location: (optional) a path, region or uri reference that can be used to identify location of resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        return self._set_persist_contract(connector_name=self.CONNECTOR_EVENTS, path=path, location=location,
                                          module_name=module_name, handler=handler, **kwargs)

    def _set_persist_contract(self, connector_name: str, path: str=None, location: str=None, module_name: str=None,
                              handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param connector_name: the name of the connector for reference
        :param path: (optional) the path to the persist resources. default 'persist/{book_name}/'
        :param location: (optional) a path, region or uri reference that can be used to identify location of resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        if connector_name not in [self.CONNECTOR_BOOK, self.CONNECTOR_EVENTS]:
            raise ValueError("The connector name must be either {} or {}. passed {}".format(self.CONNECTOR_BOOK,
                                                                                            self.CONNECTOR_EVENTS,
                                                                                            connector_name))
        reference = connector_name if self.book_pm.has_connector(connector_name) else self.CONNECTOR_PROPERTIES
        label = 'book' if connector_name == self.CONNECTOR_BOOK else 'event'
        path = os.path.join('persist', self.book_name) if not isinstance(path, str) else path
        name, connector_type = self.generate_resource_name(label=label)
        resource = os.path.join(path, name)
        if not isinstance(location, str):
            location = self.book_pm.get_connector_contract(reference).location
        if not isinstance(module_name, str):
            module_name = self.book_pm.get_connector_contract(reference).module_name
        if not isinstance(handler, str):
            handler = self.book_pm.get_connector_contract(reference).handler
        # remove the connector and handler
        if self.book_pm.has_connector(connector_name):
            self.book_pm.remove_connector_contract(connector_name)
        self.book_pm.set_connector_contract(connector_name, resource=resource, connector_type=connector_type,
                                            location=location, module_name=module_name, handler=handler, **kwargs)
        self.book_pm.persist_properties()
        return

    def reset_state(self):
        """resets the state from last persisted and applies any events from the event log"""
        if self.book_pm.has_connector(self.CONNECTOR_BOOK):
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_BOOK)
            self.__book_state = handler.load_canonical()
        else:
            self.__book_state = pd.DataFrame()
        if self.book_pm.has_connector(self.CONNECTOR_EVENTS):
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_EVENTS)
            self.__events_log = handler.load_canonical()
            _event_times = pd.Series(list(self.__events_log.keys())).sort_values().reset_index(drop=True)
            for _items in _event_times:
                _action, _event = self.__events_log.get(_items, ['add', pd.DataFrame()])
                if str(_action).lower() == 'add':
                    self.add_event(event=_event)
                elif str(_action).lower() == 'increment':
                    self.increment_event(event=_event)
                elif str(_action).lower() == 'decrement':
                    self.decrement_event(event=_event)
        else:
            self.__events_log = dict()
        return

    def report_connectors(self, stylise: bool=True):
        """ generates a report on the source contract

        :param stylise: (optional) returns a stylised dataframe with formatting
        :return: pd.DataFrame
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame()
        join = self.book_pm.join
        dpm = self.book_pm
        df['param'] = ['connector_name', 'resource', 'connector_type', 'location', 'module_name',
                       'handler', 'modified', 'kwargs']
        for name_key in dpm.get(join(dpm.KEY.connectors_key)).keys():
            connector_contract = dpm.get_connector_contract(name_key)
            if isinstance(connector_contract, ConnectorContract):
                if name_key == self.CONNECTOR_EVENTS:
                    label = 'Events Source'
                elif name_key == self.CONNECTOR_BOOK:
                    label = 'State Source'
                elif name_key == self.CONNECTOR_PROPERTIES:
                    label = 'Property Source'
                else:
                    label = name_key
                kwargs = ''
                if isinstance(connector_contract.kwargs, dict):
                    for k, v in connector_contract.kwargs.items():
                        if len(kwargs) > 0:
                            kwargs += "  "
                        kwargs += "{}='{}'".format(k, v)
                df[label] = [
                    name_key,
                    connector_contract.resource,
                    connector_contract.connector_type,
                    connector_contract.location,
                    connector_contract.module_name,
                    connector_contract.handler,
                    dpm.get(join(dpm.KEY.connectors_key, name_key, 'modified')) if dpm.is_key(
                        join(dpm.KEY.connectors_key, name_key, 'modified')) else '',
                    kwargs
                ]
        if stylise:
            df_style = df.style.set_table_styles(style).set_properties(**{'text-align': 'left'})
            _ = df_style.set_properties(subset=['param'], **{'font-weight': 'bold'})
            return df_style
        return df

    def set_version(self, version):
        """ sets the version
        :param version: the version to be set
        """
        self.book_pm.set_version(version=version)
        self.book_pm.persist_properties()
        return

    def set_persist_counters(self, state: int, events: int=None):
        """ sets the temporal counters of when to persist state and, optionally, when to persist events. Setting
        any counter to zero represents no persistence.
        note that each time the state is persisted the events log is cleared so only events since last state
        persistence are recorded. if state is set to zero then events are zero by default

        :param state: the counter for how many events to distance before persisting state
        :param events: (optional) how often to persist events e.g. if set to 1 then every event is persisted.
        """
        state = state if isinstance(state, int) else 0
        events = events if isinstance(events, int) else 0
        if not isinstance(state, int) or state < 1:
            events = 0
        self.book_pm.set_book_distance(state)
        self.book_pm.set_event_distance(events)
        self.book_pm.persist_properties()
        return


class EventBookPortfolio(object):

    MASTER_BOOK = 'master_book'
    __book_portfolio = dict()

    @singleton
    def __new__(cls):
        return super().__new__(cls)

    @classmethod
    def portfolio(cls):
        return list(cls.__book_portfolio.keys())

    @classmethod
    def is_event_book(cls, book: str=None) -> bool:
        book = cls.MASTER_BOOK if not isinstance(book, str) else book
        if book in cls.__book_portfolio.keys() and isinstance(cls.__book_portfolio.get(book), EventBook):
            return True
        return False

    @classmethod
    def get_event_book(cls, book: str=None) -> EventBook:
        book = cls.MASTER_BOOK if not isinstance(book, str) else book
        if not cls.is_event_book(book=book):
            raise ValueError("The event book instance '{}' can't be found in the portfolio.".format(book))
        return cls.__book_portfolio.get(book)

    @classmethod
    def set_event_book(cls, event_book: EventBook, book: str=None):
        if not isinstance(event_book, EventBook):
            raise TypeError("The 'event_book' must be an EventBook instance")
        book = cls.MASTER_BOOK if not isinstance(book, str) else book
        cls.remove_event_book()
        with threading.Lock():
            cls.__book_portfolio.update({book: event_book})
        return

    @classmethod
    def remove_event_book(cls, book: str=None) -> bool:
        book = cls.MASTER_BOOK if not isinstance(book, str) else book
        if book in cls.__book_portfolio.keys():
            with threading.Lock():
                cls.__book_portfolio.pop(book)
            return True
        return False

    @classmethod
    def current_state(cls, book: str=None) -> (datetime, pd.DataFrame):
        event_book = cls.get_event_book(book=book)
        return event_book.current_state

    @classmethod
    def add_event(cls, event: pd.DataFrame, book: str=None):
        with threading.Lock():
            _time = cls.get_event_book(book=book).add_event(event=event)
        return _time

    @classmethod
    def increment_event(cls, event: pd.DataFrame, book: str=None):
        with threading.Lock():
            _time = cls.get_event_book(book=book).increment_event(event=event)
        return _time

    @classmethod
    def decrement_event(cls, event: pd.DataFrame, book: str=None):
        with threading.Lock():
            _time = cls.get_event_book(book=book).decrement_event(event=event)
        return _time
