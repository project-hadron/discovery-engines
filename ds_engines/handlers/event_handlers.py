import pandas as pd
from aistac.handlers.abstract_handlers import AbstractSourceHandler, AbstractPersistHandler
from aistac.handlers.abstract_handlers import ConnectorContract, HandlerFactory
from ds_engines.engines.event_books.pandas_event_book import PandasEventBook
from ds_engines import EventBookPortfolio


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
        if self._book_name not in self._portfolio.active_books:
            self._portfolio.start_portfolio()

    def supported_types(self) -> list:
        return ['pd.DataFrame']

    def exists(self) -> bool:
        return self._portfolio.is_event_book(self._book_name)

    def has_changed(self) -> bool:


    def reset_changed(self, changed: bool = False):
        pass

    def load_canonical(self, **kwargs) -> pd.DataFrame:
        pass


class EventPersistHandler(EventSourceHandler, AbstractPersistHandler):


    def persist_canonical(self, canonical: pd.DataFrame, **kwargs) -> bool:
        pass

    def remove_canonical(self, **kwargs) -> bool:
        pass

    def backup_canonical(self, canonical: pd.DataFrame, uri: str, **kwargs) -> bool:
        pass
