from datetime import datetime
from typing import Any
import pandas as pd
from ds_foundation.components.abstract_component import AbstractComponent
from ds_foundation.handlers.abstract_event_book import AbstractEventBook
from ds_foundation.handlers.abstract_handlers import ConnectorContract
from ds_engines.managers.event_book_property_manager import EventBookPropertyManager
from ds_engines.intent.event_book_intent_model import EventBookIntentModel

__author__ = 'Darryl Oatridge'


class EventBookPortfolio(AbstractComponent):

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
        """returns a list of portfolio event names"""
        return list(self.__book_portfolio.keys())

    def set_event_book(self, book_name: str, module_name: str=None, event_book_cls: str=None,
                       create_book: bool=None, intent_level: [int, str]=None, **kwargs):
        """ sets an event book as a parameterised intent

        :param book_name: the unique reference name for the event book
        :param module_name: (optional) a package path for an Event Book class
        :param event_book_cls: (optional) a concrete implmentation of the AbstractEventBook
        :param create_book: (optiona) if an instance of the Evetn Book should be created and added to the portfolio.
        :param intent_level: (optional) an intent level to put the parameterised intent
        :param kwargs: any kwargs to be pass as part of the parameterised intent
        """
        if not isinstance(book_name, str) or len(book_name) == 0:
            raise ValueError("The book_name must be a valid string")
        if self.is_event_book(book_name=book_name):
            raise KeyError(f"The book name '{book_name}' already exists in the portfolio")
        result = self.intent_model.set_event_book(book_name=book_name, save_intent=True, create_book=create_book,
                                                  module_name=module_name, event_book_cls=event_book_cls,
                                                  intent_level=intent_level, replace_intent=True, **kwargs)
        if isinstance(result, AbstractEventBook):
            self.__book_portfolio.update({book_name: result})
        return

    def update_portfolio(self, run_book: [str, list]=None):
        """runs the intent pipeline to create the portfolio. Optionally a list of Intent levels can
         be passed to selectively run certain event books. If an event book already exists it won't be replaced.
         To remove an evetn book, explicitely 'remove_event_book(...)'"""
        if isinstance(run_book, (str, list)):
            run_book = self.pm.list_formatter(run_book)
        else:
            run_book = None
        self.__book_portfolio.update(self.intent_model.run_intent_pipeline(run_book=run_book, exclude=self.portfolio))
        return

    def is_event_book(self, book_name: str) -> bool:
        """Tests if an event book exists and if it is of type AbstractEventBook"""
        if book_name in self.__book_portfolio.keys() and isinstance(self.__book_portfolio.get(book_name),
                                                                    AbstractEventBook):
            return True
        return False

    def get_event_book(self, book_name: str):
        """retrieves an event book instance from the portfolio by name"""
        if not self.is_event_book(book_name=book_name):
            raise ValueError(f"The event book instance '{book_name}' can't be found in the portfolio.")
        return self.__book_portfolio.get(book_name)

    def set_event_book_connectors(self, book_name: str, state_connector: ConnectorContract,
                                  events_log_connector: ConnectorContract):
        """sets a pair of connectors for the state and event log. The connectors will have the name of the book
        with a events log connector having a suffix of '_log'

        :param book_name: the unique name of the event book
        :param state_connector: the state connector contract
        :param events_log_connector: (optional) the events log connector contract
        """
        state_name = book_name
        events_log_name = "_".join([book_name, '_log'])
        if isinstance(state_connector, ConnectorContract):
            if self.has_connector_contract(state_name):
                self.remove_connector_contract(connector_name=state_name)
            self.set_connector_contract(connector_name=state_name, connector_contract=state_connector)
        if isinstance(events_log_connector, ConnectorContract):
            if self.has_connector_contract(events_log_name):
                self.remove_connector_contract(connector_name=events_log_name)
            self.set_connector_contract(connector_name=events_log_name, connector_contract=events_log_connector)
        return

    def remove_event_book(self, book_name: str):
        """removes the event book"""
        state_name = book_name
        events_log_name = "_".join([book_name, '_log'])
        # remove the connectors
        if self.has_connector_contract(state_name):
            self.remove_connector_contract(state_name)
        if self.has_connector_contract(events_log_name):
            self.remove_connector_contract(events_log_name)
        # remove the intent
        self.pm.remove_intent(intent_param=book_name)
        # remove the portfolio entry
        self.__book_portfolio.pop(book_name)
        if book_name in self.__book_portfolio.keys():
            self.__book_portfolio.pop(book_name)
        return False

    def current_state(self, book_name: str) -> (datetime, Any):
        event_book = self.get_event_book(book_name=book_name)
        return event_book.current_state

    def add_event(self, book_name: str, event: Any):
        return self.get_event_book(book_name=book_name).add_event(event=event)

    def increment_event(self, book_name: str, event: Any):
        return self.get_event_book(book_name=book_name).increment_event(event=event)

    def decrement_event(self, book_name: str, event: Any):
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
