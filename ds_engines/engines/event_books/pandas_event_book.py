from copy import deepcopy
from datetime import datetime
import pandas as pd
from aistac.handlers.abstract_event_book import AbstractEventBook
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory

__author__ = 'Darryl Oatridge'


class PandasEventBook(AbstractEventBook):

    __book_state: pd.DataFrame
    __events_log: dict
    __event_count: int
    __book_count: int
    __last_book_time: datetime

    def __init__(self, book_name: str, time_distance: int=None, count_distance: int=None, events_log_distance: int=None,
                 state_connector: ConnectorContract=None, events_log_connector: ConnectorContract=None):
        """ Encapsulation class for the management of Event Books

        :param book_name: The name of the event book
        :param time_distance: the time distance for persisting the state
        :param count_distance: the count distance for persisting the state (only if no time distance)
        :param events_log_distance: the log event distance. This is for percistence recovery.
        :param state_connector: The persist handler for the state book
        :param events_log_connector: The persist handler for the event log
        """
        if not isinstance(book_name, str) or len(book_name) < 1:
            raise ValueError("The contract name must be a valid string")
        super().__init__(book_name=book_name)
        self._book_name = book_name
        self._state_connector = state_connector
        self._events_connector = events_log_connector
        self._time_distance = time_distance if isinstance(time_distance, int) else 0
        self._count_distance = count_distance if isinstance(count_distance, int) else 0
        self._events_log_distance = events_log_distance if isinstance(events_log_distance, int) else 0
        # initialise the globals
        self.__book_state = pd.DataFrame()
        self.__events_log = dict()
        self.__event_count = 0
        self.__book_count = 0
        self.__last_book_time = datetime.now()

    @property
    def count_distance(self) -> int:
        """returns the current state count distance"""
        return self._count_distance

    @property
    def time_distance(self) -> int:
        """returns the current state time distance in seconds"""
        return self._time_distance

    @property
    def events_log_distance(self) -> int:
        """returns the current events log distance"""
        return self._events_log_distance

    def set_count_distance(self, distance: int):
        """sets the state count distance"""
        self._count_distance = distance

    def set_time_distance(self, distance: int):
        """sets the state time distance as seconds"""
        self._time_distance = distance

    def set_events_log_distance(self, distance: int):
        """sets the state events log distance."""
        self._events_log_distance = distance

    @property
    def current_state(self) -> (datetime, pd.DataFrame):
        return datetime.now(), self.__book_state.copy(deep=True)

    @property
    def _current_events_log(self) -> dict:
        return deepcopy(self.__events_log)

    def add_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        if self.events_log_distance > 0:
            self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['add', event]})
        self.__book_state = event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def increment_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        if self.events_log_distance > 0:
            self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['increment', event]})
        _event = event.combine(self.__book_state, lambda s1, s2: s2 + s1 if len(s2.mode()) else s1)
        self.__book_state = _event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def decrement_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        if self.events_log_distance > 0:
            self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): ['decrement', event]})
        _event = event.combine(self.__book_state, lambda s1, s2: s2 - s1 if len(s2.mode()) else s1)
        self.__book_state = _event.combine_first(self.__book_state)
        self._update_counters()
        return _time

    def _update_counters(self):
        self.__book_count += 1 if self._count_distance > 0 else 0
        self.__event_count += 1 if self._events_log_distance > 0 else 0
        book_update = False
        if 0 < self._time_distance <= (datetime.now() - self.__last_book_time).total_seconds():
            self.__last_book_time = datetime.now()
            book_update = True
        elif 0 < self._count_distance <= self.__book_count:
            self.__book_count = 0
            book_update = True
        if book_update:
            self.persist_book()
            self._persist_events()
            self.__events_log = dict()
            self.__event_count = 0
        if 0 < self._events_log_distance <= self.__event_count:
            self.__event_count = 0
            self._persist_events()
            self.__events_log = dict()
        return

    def persist_book(self, **kwargs):
        """ persists the event book state """
        if isinstance(self._state_connector, ConnectorContract):
            _current_state = self.current_state[1]
            handler = HandlerFactory.instantiate(self._state_connector)
            handler.persist_canonical(_current_state, **kwargs)
        return

    def persist_backup_book(self, stamp_uri: str=None, **kwargs):
        """ persists the event book state with an alternative to save off a stamped copy to a provided URI

        :param stamp_uri: in addition to persisting the event book, save to this uri
        :return:
        """
        if isinstance(self._state_connector, ConnectorContract):
            _current_state = self.current_state[1]
            handler = HandlerFactory.instantiate(self._state_connector)
            if isinstance(stamp_uri, str):
                handler.backup_canonical(canonical=_current_state, uri=stamp_uri, **kwargs)
        return

    def _persist_events(self):
        """Saves the pandas.DataFrame to the persisted stater"""
        if isinstance(self._events_connector, ConnectorContract):
            handler = HandlerFactory.instantiate(self._events_connector)
            handler.persist_canonical(self.__events_log)
        return

    def reset_state(self):
        """resets the state from last persisted and applies any events from the event log"""
        if isinstance(self._state_connector, ConnectorContract):
            handler = HandlerFactory.instantiate(self._state_connector)
            self.__book_state = handler.load_canonical()
        else:
            self.__book_state = pd.DataFrame()
        if isinstance(self._events_connector, ConnectorContract):
            handler = HandlerFactory.instantiate(self._events_connector)
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
