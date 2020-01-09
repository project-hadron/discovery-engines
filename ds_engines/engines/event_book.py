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
        self._book_pm = EventBookPropertyManager.from_properties(contract_name=self._book_name,
                                                                 connector_contract=event_book_properties)
        self.CONNECTOR_PROPERTIES = self._book_pm.CONNECTOR_INTENT
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
    def from_uri(cls, properties_uri: str, book_name: str=None):
        """ Class Factory Method that builds the connector handlers for the properties contract. The method uses
        the schema of the URI to determine if it is remote or local. s3:// schema denotes remote, empty schema denotes
        local.
        Note: the 'properties_uri' only provides a URI up to and including the path but not the properties file names.

         :param book_name: The reference name of the properties contract
         :param properties_uri: A URI that identifies the resource path. The syntax should be either
                          s3://<bucket>/<path>/ for remote or <path> for local
         :return: the initialised class instance
         """
        _book_name = book_name if isinstance(book_name, str) else 'eb_primary_book'
        _uri = properties_uri
        if not isinstance(_uri, str) or len(_uri) == 0:
            raise ValueError("the URI must take the form 's3://<bucket>/<path>/' for remote or '<path>/' for local")
        _schema, _netloc, _path = ConnectorContract.parse_address_elements(uri=_uri)
        if str(_schema).lower().startswith('s3'):
            return cls._from_remote(book_name=book_name, properties_uri=_uri)
        _uri = _path
        if not os.path.exists(_path):
            os.makedirs(_path, exist_ok=True)
        return cls._from_local(book_name=book_name, properties_uri=_uri)

    @classmethod
    def from_env(cls, book_name: str=None):
        """ Class Factory Method that builds the connector handlers taking the property contract path from
        the os.envon['AISTAC_TR_URI'] or locally from the current working directory './' if
        no environment variable is found. This assumes the use of the pandas handler module and yaml persisted file.

         :param book_name: The reference name of the properties contract
         :return: the initialised class instance
         """
        book_name = book_name if isinstance(book_name, str) else 'eb_primary_book'
        if 'AISTAC_INTENT' in os.environ.keys():
            properties_uri = os.environ['AISTAC_INTENT']
        else:
            properties_uri = "/tmp/aistac/eventbook/contracts"
        return cls.from_uri(book_name=book_name, properties_uri=properties_uri)

    @classmethod
    def _from_remote(cls, book_name: str, properties_uri: str):
        """ Class Factory Method that builds the connector handlers an Amazon AWS s3 remote store.
        Note: the 'properties_uri' only provides a URI up to and including the path but not the properties file names.

         :param book_name: The reference name of the properties contract
         :param properties_uri: A URI that identifies the S3 properties resource path. The syntax should be:
                          s3://<bucket>/<path>/
         :return: the initialised class instance
         """
        if not isinstance(book_name, str) or len(book_name) == 0:
            raise ValueError("A contract_name must be provided")
        _module_name = 'ds_discovery.handlers.aws_s3_handlers'
        _handler = 'AwsS3PersistHandler'
        _address = ConnectorContract.parse_address(uri=properties_uri)
        _query_kw = ConnectorContract.parse_query(uri=properties_uri)
        _data_uri = os.path.join(_address, "config_event_book_{}.pickle".format(book_name))
        _data_connector = ConnectorContract(uri=_data_uri, module_name=_module_name, handler=_handler, **_query_kw)
        return cls(book_name=book_name, event_book_properties=_data_connector)

    @classmethod
    def _from_local(cls, book_name: str,  properties_uri: str, default_save=None):
        """ Class Factory Method that builds the connector handlers from a local resource path.
        This assumes the use of the pandas handler module and yaml persisted file.

        :param book_name: The reference name of the properties contract
        :param properties_uri: (optional) A URI that identifies the properties resource path.
                            by default is '/tmp/aistac/contracts'
        :param default_save: (optional) if the configuration should be persisted
        :return: the initialised class instance
        """
        if not isinstance(book_name, str) or len(book_name) == 0:
            raise ValueError("A contract_name must be provided")
        _properties_uri = properties_uri if isinstance(properties_uri, str) else "/tmp/aistac/contracts"
        _default_save = default_save if isinstance(default_save, bool) else True
        _module_name = 'ds_discovery.handlers.pandas_handlers'
        _handler = 'PandasPersistHandler'
        _data_uri = os.path.join(properties_uri, "config_transition_data_{}.yaml".format(book_name))
        _data_connector = ConnectorContract(uri=_data_uri, module_name=_module_name, handler=_handler)
        return cls(book_name=book_name, event_book_properties=_data_connector)

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

    def persist_book(self, stamp_uri: str=None, ignore_kwargs: bool=False):
        """ persists the event book state with an alternative to save off a stamped copy to a provided URI

        :param stamp_uri: in addition to persisting the event book, save to this uri
        :param ignore_kwargs: if the ConnectContract kwargs should be ignored and the query value pairs used
        :return:
        """
        if self.book_pm.has_connector(self.CONNECTOR_BOOK):
            _current_state = self.current_state[1]
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_BOOK)
            handler.persist_canonical(_current_state)
            if isinstance(stamp_uri, str):
                handler.backup_canonical(canonical=_current_state, uri=stamp_uri, ignore_kwargs=ignore_kwargs)
            return
        raise ConnectionError("The 'State' Connector Contract has not been set, see 'set_state_connector_contract()'")

    def _persist_events(self):
        """Saves the pandas.DataFrame to the persisted stater"""
        if self.book_pm.has_connector(self.CONNECTOR_EVENTS):
            handler = self.book_pm.get_connector_handler(self.CONNECTOR_EVENTS)
            handler.persist_canonical(self.__events_log)
            return
        raise ConnectionError("The 'Events' Connector Contract has not been set, see 'set_events_connector_contract()'")

    def set_state_persist_contract(self, uri: str=None, module_name: str=None, handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param uri: A Uniform Resource Identifier that unambiguously identifies a particular resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class found within the module
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        if not isinstance(module_name, str):
            module_name = self.book_pm.get_connector_contract(self.CONNECTOR_PROPERTIES).module_name
        if not isinstance(handler, str):
            handler = self.book_pm.get_connector_contract(self.CONNECTOR_PROPERTIES).handler
        # remove the connector and handler
        if self.book_pm.has_connector(self.CONNECTOR_BOOK):
            self.book_pm.remove_connector_contract(self.CONNECTOR_BOOK)
        self.book_pm.set_connector_contract(self.CONNECTOR_BOOK, uri=uri, module_name=module_name, handler=handler,
                                            **kwargs)
        self.book_pm.persist_properties()
        return

    def set_events_persist_contract(self, uri: str=None, module_name: str=None, handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param uri: A Uniform Resource Identifier that unambiguously identifies a particular resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        if not isinstance(module_name, str):
            module_name = self.book_pm.get_connector_contract(self.CONNECTOR_PROPERTIES).module_name
        if not isinstance(handler, str):
            handler = self.book_pm.get_connector_contract(self.CONNECTOR_PROPERTIES).handler
        # remove the connector and handler
        if self.book_pm.has_connector(self.CONNECTOR_EVENTS):
            self.book_pm.remove_connector_contract(self.CONNECTOR_EVENTS)
        self.book_pm.set_connector_contract(self.CONNECTOR_EVENTS, uri=uri, module_name=module_name, handler=handler,
                                            **kwargs)
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
        df['param'] = ['connector_name', 'uri', 'module_name', 'handler', 'modified', 'kwargs', 'query', 'params']
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
                query = ''
                if isinstance(connector_contract.query, dict):
                    for k, v in connector_contract.query.items():
                        if len(query) > 0:
                            query += "  "
                        query += "{}='{}'".format(k, v)
                df[label] = [
                    name_key,
                    connector_contract.address,
                    connector_contract.module_name,
                    connector_contract.handler,
                    kwargs,
                    query,
                    connector_contract.params,
                    dpm.get(join(dpm.KEY.connectors_key, name_key, 'modified')) if dpm.is_key(
                        join(dpm.KEY.connectors_key, name_key, 'modified')) else '',
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
