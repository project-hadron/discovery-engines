from datetime import datetime

import pandas as pd
from ds_engines.event_book.event_book import EventBook

from ds_foundation.components.abstract_component import AbstractComponent
from ds_foundation.handlers.abstract_handlers import ConnectorContract

from ds_engines.managers.event_book_property_manager import EventBookPropertyManager
from ds_engines.intent.event_book_intent_model import EventBookIntentModel

__author__ = 'Darryl Oatridge'


class EventBookPortfolio(AbstractComponent):

    CONNECTOR_BOOK = 'book'
    CONNECTOR_EVENTS = 'events'

    __book_portfolio = dict()

    def __init__(self, property_manager: EventBookPropertyManager, default_save=None):
        """ Encapsulation class for the discovery set of classes

        :param property_manager: The contract property manager instance for this component
        :param default_save: The default behaviour of persisting the contracts:
                        if False: The connector contracts are kept in memory (useful for restricted file systems)
        """
        _intent_model = EventBookIntentModel(property_manager=property_manager)

        super().__init__(property_manager=property_manager, intent_model=_intent_model, default_save=default_save)

    @classmethod
    def from_uri(cls, task_name: str, uri_pm_path: str, default_save=None, **kwargs):
        _pm = EventBookPropertyManager(task_name=task_name)
        super()._init_properties(property_manager=_pm, uri_pm_path=uri_pm_path, **kwargs)
        return cls(property_manager=_pm, default_save=default_save)

    @classmethod
    def _from_remote_s3(cls) -> (str, str):
        """ Class Factory Method that builds the connector handlers an Amazon AWS s3 remote store."""
        _module_name = 'ds_connectors.handler.aws_s3_handlers'
        _handler = 'AwsS3PersistHandler'
        return _module_name, _handler

    @property
    def intent_model(self) -> EventBookIntentModel:
        """The intent model instance"""
        return self._intent_model

    @property
    def pm(self) -> EventBookPropertyManager:
        """The properties manager instance"""
        return self._component_pm

    @property
    def portfolio(self):
        return list(self.__book_portfolio.keys())

    def update_portfolio(self, run_book: [str, list]=None):
        """runs the intent pipeline to create the portfolio. Optionally a list of Intent level profolios can
         be passed to selectively run certain event books"""
        if isinstance(run_book, (str, list)):
            run_book = self.pm.list_formatter(run_book)
        else:
            run_book = None
        self.__book_portfolio.update(self.intent_model.run_intent_pipeline(run_book=run_book))

    def is_event_book(self, book_name: str) -> bool:
        if book_name in self.__book_portfolio.keys() and isinstance(self.__book_portfolio.get(book_name), EventBook):
            return True
        return False

    def get_event_book(self, book_name: str) -> EventBook:
        if not self.is_event_book(book_name=book_name):
            raise ValueError("The event book instance '{}' can't be found in the portfolio.".format(book_name))
        return self.__book_portfolio.get(book_name)

    def set_event_book(self, book_name: str, time_distance: int=None, events_distance: int=None,
                       count_distance: int=None, state_connector: ConnectorContract=None,
                       events_log_connector: ConnectorContract=None):
        """sets the event book as an intent, setting the connectors if given

        :param book_name: the unique name of the event book
        :param time_distance: (optional) a time distance to persist the event book state
        :param count_distance: (optional) an event count distance to persist the event book state
        :param events_distance: (optional) an event distance to record the events log
        :param state_connector: (optional) the state connector contract
        :param events_log_connector: (optional) the events log connector contract
        """
        state_name = "_".join(["state", book_name])
        events_log_name = "_".join(["events_log", book_name])
        if isinstance(state_connector, ConnectorContract):
            if self.has_connector_contract(state_name):
                self.remove_connector_contract(connector_name=state_name)
            self.set_connector_contract(connector_name=state_name, connector_contract=state_connector)
        if isinstance(state_connector, ConnectorContract):
            if self.has_connector_contract(events_log_name):
                self.remove_connector_contract(connector_name=events_log_name)
            self.set_connector_contract(connector_name=events_log_name, connector_contract=events_log_connector)
        # set the intent
        self.intent_model.set_event_book(book_name=book_name, time_distance=time_distance,
                                         events_distance=events_distance,count_distance=count_distance,
                                         state_connector=state_name, events_log_connector=events_log_name)
        return

    def remove_event_book(self, book_name: str):
        """removes the event book"""
        state_name = "_".join(["state", book_name])
        events_log_name = "_".join(["events_log", book_name])
        # remove the connectors
        if self.has_connector_contract(state_name):
            self.remove_connector_contract(state_name)
        if self.has_connector_contract(events_log_name):
            self.remove_connector_contract(events_log_name)
        # remove the intent
        if self.has_intent(book_name):
            self.remove_intent(book_name)
        # remove the portfolio entry
        self.__book_portfolio.pop(book_name)
        if book_name in self.__book_portfolio.keys():
            self.__book_portfolio.pop(book_name)
        return False

    def current_state(self, book_name: str) -> (datetime, pd.DataFrame):
        event_book = self.get_event_book(book_name=book_name)
        return event_book.current_state

    def add_event(self, book_name: str, event: pd.DataFrame):
        return self.get_event_book(book_name=book_name).add_event(event=event)

    def increment_event(self, book_name: str, event: pd.DataFrame):
        return self.get_event_book(book_name=book_name).increment_event(event=event)

    def decrement_event(self, book_name: str, event: pd.DataFrame):
        return self.get_event_book(book_name=book_name).decrement_event(event=event)

    def report_connectors(self, connector_filter: [str, list]=None, stylise: bool=True):
        """ generates a report on the source contract

        :param connector_filter: (optional) filters on the connector name.
        :param stylise: (optional) returns a stylised DataFrame with formatting
        :return: pd.DataFrame
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame.from_dict(data=self.pm.report_connectors(connector_filter=connector_filter), orient='columns')
        if stylise:
            df_style = df.style.set_table_styles(style).set_properties(**{'text-align': 'left'})
            _ = df_style.set_properties(subset=['connector_name'], **{'font-weight': 'bold'})
            return df_style
        else:
            df.set_index(keys='connector_name', inplace=True)
        return df

    def report_run_book(self, stylise: bool=True):
        """ generates a report on all the intent

        :param stylise: returns a stylised dataframe with formatting
        :return: pd.Dataframe
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame.from_dict(data=self.pm.report_run_book(), orient='columns')
        if stylise:
            index = df[df['name'].duplicated()].index.to_list()
            df.loc[index, 'name'] = ''
            df = df.reset_index(drop=True)
            df_style = df.style.set_table_styles(style).set_properties(**{'text-align': 'left'})
            _ = df_style.set_properties(subset=['name'],  **{'font-weight': 'bold', 'font-size': "120%"})
            return df_style
        return df

    def report_intent(self, stylise: bool=True):
        """ generates a report on all the intent

        :param stylise: returns a stylised dataframe with formatting
        :return: pd.Dataframe
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame.from_dict(data=self.pm.report_intent(), orient='columns')
        if stylise:
            index = df[df['level'].duplicated()].index.to_list()
            df.loc[index, 'level'] = ''
            df = df.reset_index(drop=True)
            df_style = df.style.set_table_styles(style).set_properties(**{'text-align': 'left'})
            _ = df_style.set_properties(subset=['level'],  **{'font-weight': 'bold', 'font-size': "120%"})
            return df_style
        return df

    def report_notes(self, catalog: [str, list]=None, labels: [str, list]=None, regex: [str, list]=None,
                     re_ignore_case: bool=False, stylise: bool=True, drop_dates: bool=False):
        """ generates a report on the notes

        :param catalog: (optional) the catalog to filter on
        :param labels: (optional) s label or list of labels to filter on
        :param regex: (optional) a regular expression on the notes
        :param re_ignore_case: (optional) if the regular expression should be case sensitive
        :param stylise: (optional) returns a stylised dataframe with formatting
        :param drop_dates: (optional) excludes the 'date' column from the report
        :return: pd.Dataframe
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        drop_dates = False if not isinstance(drop_dates, bool) else drop_dates
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        report = self.pm.report_notes(catalog=catalog, labels=labels, regex=regex, re_ignore_case=re_ignore_case,
                                      drop_dates=drop_dates)
        df = pd.DataFrame.from_dict(data=report, orient='columns')
        if stylise:
            df_style = df.style.set_table_styles(style).set_properties(**{'text-align': 'left'})
            _ = df_style.set_properties(subset=['section'], **{'font-weight': 'bold'})
            _ = df_style.set_properties(subset=['label', 'section'], **{'font-size': "120%"})
            return df_style
        return df
