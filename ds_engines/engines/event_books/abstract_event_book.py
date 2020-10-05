import importlib.util
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

__author__ = 'Darryl Oatridge'


class EventBookContract(object):
    """A container class for the event handler"""
    book_name: str = None
    module_name: str = None
    event_book_cls: str = None
    _kwargs: dict = None

    def __init__(self, book_name: str, module_name: str, event_book_cls: str, **kwargs):
        self.book_name = book_name
        self.module_name = module_name
        self.event_book_cls = event_book_cls
        self._kwargs = kwargs if isinstance(kwargs, dict) else {}

    @property
    def kwargs(self) -> dict:
        """copy of the private kwargs dictionary"""
        return self._kwargs.copy() if isinstance(self._kwargs, dict) else {}

    @staticmethod
    def from_dict(event_book_dict: dict):
        """returns a new event contract from a dictionary"""
        if not isinstance(event_book_dict, dict):
            event_book_dict = {}
        book_name = event_book_dict.get('book_name', 'primary_book')
        module_name = event_book_dict.get('module_name', 'ds_engines.engines.event_books.pandas_event_book')
        event_book_cls = event_book_dict.get('event_book_cls', 'PandasEventBook')
        kwargs = event_book_dict.get('kwargs', {})
        return EventBookContract(book_name=book_name, module_name=module_name, event_book_cls=event_book_cls, **kwargs)

    def to_dict(self) -> dict:
        """Returns a dictionary representation of the Connector Contract"""
        rtn_dict = {'book_name': self.book_name, 'module_name': self.module_name, 'event_book_cls': self.event_book_cls,
                    'kwargs': self.kwargs}
        return rtn_dict

    def __len__(self):
        return self.to_dict().__len__()

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return "<{} {}".format(self.__class__.__name__, str(self.to_dict()))

    def __eq__(self, other):
        return self.to_dict().__eq__(other.to_dict())

    def __setattr__(self, key, value):
        if self.to_dict().get(key, None) is None:
            super().__setattr__(key, value)
        else:
            raise AttributeError("The attribute '{}' is immutable once set and can not be changed".format(key))

    def __delattr__(self, item):
        raise AttributeError("{} is an immutable class and attributes can't be removed".format(self.__class__.__name__))


class AbstractEventBook(ABC):

    @abstractmethod
    def __init__(self, book_name: str):
        """instantiates a book event"""
        self._book_name = book_name
        self._modified_flag = False

    @property
    def book_name(self) -> str:
        """The book name of this transition instance"""
        return self._book_name

    @property
    def modified(self) -> bool:
        """A boolean flag that is raised when a modifier method is called"""
        return self._modified_flag

    def reset_modified(self):
        """resets the modifier flag to be lowered"""
        self._modified_flag = False

    def _set_modified(self, flag: bool):
        """ sets the modified flag"""
        self._modified_flag = flag if isinstance(flag, bool) else False

    @abstractmethod
    def current_state(self, fillna: bool=None) -> (datetime, Any):
        """returns a tuple of datetime and the current book state"""

    @abstractmethod
    def add_event(self, event: Any) -> datetime:
        """add an event to the event book, replacing anything in the event cells"""

    @abstractmethod
    def increment_event(self, event: Any) -> datetime:
        """add an event to the event book, incrementing the values in the event cells"""

    @abstractmethod
    def decrement_event(self, event: Any) -> datetime:
        """add an event to the event book, decrementing the values in the event cells"""

    @abstractmethod
    def reset_state(self):
        """resets the event book to its starting state"""


class EventBookFactory(object):

    @staticmethod
    def instantiate(event_book_contract: EventBookContract) -> [AbstractEventBook]:
        book_name = event_book_contract.book_name
        module_name = event_book_contract.module_name
        event_book_cls = event_book_contract.event_book_cls

        # check module
        module_spec = importlib.util.find_spec(module_name)
        if module_spec is None:
            raise ModuleNotFoundError("The module '{}' could not be found".format(module_name))

        # check event_book_cls
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        if event_book_cls not in dir(module):
            raise ImportError(f"The event_book_cls '{event_book_cls}' could not be found in the module '{module_name}'")

        # create instance of event_book_cls
        local_kwargs = locals().get('kwargs') if 'kwargs' in locals() else dict()
        local_kwargs['module'] = module
        instance = eval(f"module.{event_book_cls}(book_name='{book_name}')", globals(), local_kwargs)
        if not isinstance(instance, AbstractEventBook):
            raise TypeError(f"The event_book_cls '{event_book_cls}' is not a subset of the AbstractHandler Class")
        return instance
