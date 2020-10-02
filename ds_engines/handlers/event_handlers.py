import pandas as pd
from aistac.handlers.abstract_handlers import AbstractSourceHandler, AbstractPersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory


__author__ = 'Darryl Oatridge'


class EventSourceHandler(AbstractSourceHandler):

    def __init__(self, connector_contract: ConnectorContract):
        """ initialise the Handler passing the connector_contract dictionary

        Extra Parameters in the ConnectorContract kwargs:
            - region_name (optional) session region name
            - profile_name (optional) session shared credentials file profile name
        """
        self.engine = HandlerFactory.get_module('ds_engines')
        super().__init__(connector_contract)
        _cc = connector_contract
        # check if this is coming from a remote contract
        uri_pm_repo = _cc.kwargs.pop('uri_pm_repo', None)
        if not uri_pm_repo:
            uri_pm_repo = _cc.query.pop('uri_pm_repo', None)
        try:
            self._portfolio = self.engine.EventBookPortfolio.from_env(task_name=connector_contract.netloc,
                                                                      uri_pm_repo=uri_pm_repo)
        except FileNotFoundError:
            raise ValueError(f"The EventBook Portfolio '{connector_contract.netloc}' Domain Contract can not be found. "
                             f"If you are using a repo remember to include 'uri_pm_repo' as a query string or kwarg")
        self._book_name = connector_contract.path[1:]
        if not self._portfolio.is_event_book(self._book_name):
            raise ValueError(f"The EventBook '{self._book_name}' not found in the EventPortfolio '{self._portfolio}'")
        if not self._portfolio.is_active_book(self._book_name):
            self._portfolio.start_portfolio()

    def supported_types(self) -> list:
        return ['pd.DataFrame']

    def exists(self) -> bool:
        return self._portfolio.is_event_book(self._book_name)

    def has_changed(self) -> bool:
        if self._portfolio.is_active_book(self._book_name):
            eb = self._portfolio.get_active_book(self._book_name)
            return eb.modified
        return False

    def reset_changed(self, changed: bool=False):
        if self._portfolio.is_active_book(self._book_name):
            self._portfolio.get_active_book(self._book_name).set_modified(changed)
        return

    def load_canonical(self, **kwargs) -> pd.DataFrame:
        if not self._portfolio.is_active_book(self._book_name):
            raise ValueError(f"The EventBook '{self._book_name}' in '{self._portfolio}' is not active")
        return self._portfolio.get_active_book(self._book_name).current_state()


class EventPersistHandler(EventSourceHandler, AbstractPersistHandler):

    def persist_canonical(self, canonical: pd.DataFrame, reset_state: bool=None, **kwargs) -> bool:
        """ persists the canonical into the event book extending or replacing the current state

        :param canonical: the canonical to persist to the event book
        :param reset_state: True - resets the event book (Default)
                            False - merges the canonical to the current state based on their index
        """
        if not self._portfolio.is_event_book(self._book_name):
            raise ValueError(f"The EventBook '{self._book_name}' does not exist in '{self._portfolio}'")
        if not self._portfolio.is_active_book(self._book_name):
            self._portfolio.start_portfolio()
        reset_state = reset_state if isinstance(reset_state, bool) else True
        eb = self._portfolio.get_active_book(self._book_name)
        if reset_state:
            eb.reset_state()
        eb.add_event(event=canonical, fix_index=False)
        return True

    def remove_canonical(self, **kwargs) -> bool:
        if not self._portfolio.is_event_book(self._book_name):
            raise ValueError(f"The EventBook '{self._book_name}' does not exist in '{self._portfolio}'")
        if not self._portfolio.is_active_book(self._book_name):
            self._portfolio.start_portfolio()
        self._portfolio.get_active_book(self._book_name).reset_state()
        return True

    def backup_canonical(self, canonical: pd.DataFrame, uri: str, reset_state: bool=None, **kwargs) -> bool:
        """ persists the canonical into the event book extending or replacing the current state

        :param canonical: the canonical to persist to the event book
        :param uri: the uri of the event book
        :param reset_state: True - resets the event book (Default)
                            False - merges the canonical to the current state based on their index
        """
        schema, netloc, path = ConnectorContract.parse_address_elements(uri=uri)
        try:
            _portfolio = self.engine.EventBookPortfolio.from_env(task_name=netloc)
        except FileNotFoundError:
            raise ValueError(f"The EventBook Portfolio '{netloc}' Domain Contract can not be found. ")
        _book_name = path[1:]
        if not _portfolio.is_event_book(_book_name):
            raise ValueError(f"The EventBook '{_book_name}' not found in the EventPortfolio '{_portfolio}'")
        if not _portfolio.is_active_book(_book_name):
            _portfolio.start_portfolio()
        reset_state = reset_state if isinstance(reset_state, bool) else True
        eb = _portfolio.get_active_book(_book_name)
        if reset_state:
            eb.reset_state()
        eb.add_event(event=canonical, fix_index=False)
        return True
