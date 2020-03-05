import os
from datetime import datetime
from typing import Any
import pandas as pd
from aistac.components.abstract_component import AbstractComponent
from aistac.handlers.abstract_event_book import AbstractEventBook
from aistac.handlers.abstract_handlers import ConnectorContract
from ds_engines.managers.event_book_property_manager import EventBookPropertyManager
from ds_engines.intent.event_book_intent_model import EventBookIntentModel

__author__ = 'Darryl Oatridge'


class EventBookPortfolio(AbstractComponent):

    __book_portfolio = dict()
    BOOK_TEMPLATE_CONNECTOR = 'book_template_connector'

    def __init__(self, property_manager: EventBookPropertyManager, default_save=None, default_save_intent: bool=None,
                 intent_type_additions: list=None):
        """ Encapsulation class for the discovery set of classes

        :param property_manager: The contract property manager instance for this component
        :param default_save: (optional) The default behaviour of persisting the contracts:
                        if False: The connector contracts are kept in memory (useful for restricted file systems)
        :param intent_type_additions: (optional) if the intent has additional data_types passed as parameters
        """
        _intent_model = EventBookIntentModel(property_manager=property_manager, default_save_intent=default_save_intent,
                                             intent_type_additions=intent_type_additions)
        super().__init__(property_manager=property_manager, intent_model=_intent_model, default_save=default_save)

    @classmethod
    def from_uri(cls, task_name: str, uri_pm_path: str, pm_file_type: str = None, pm_module: str = None,
                 pm_handler: str = None, default_save=None, template_source_path: str = None,
                 template_persist_path: str = None, template_source_module: str = None,
                 template_persist_module: str = None, template_source_handler: str = None,
                 template_persist_handler: str = None, **kwargs):
        """ Class Factory Method to instantiates the component application. The Factory Method handles the
        instantiation of the Properties Manager, the Intent Model and the persistence of the uploaded properties.

        by default the handler is local Pandas but also supports remote AWS S3 and Redis. It use these Factory
        instantiations ensure that the schema is s3:// or redis:// and the handler will be automatically redirected

         :param task_name: The reference name that uniquely identifies a task or subset of the property manager
         :param uri_pm_path: A URI that identifies the resource path for the property manager.
         :param pm_file_type: (optional) defines a specific file type for the property manager
         :param default_save: (optional) if the configuration should be persisted. default to 'True'
         :param pm_module: (optional) the module or package name where the handler can be found
         :param pm_handler: (optional) the handler for retrieving the resource
         :param default_save: (optional) if the configuration should be persisted. default to 'True'
         :param template_source_path: (optional) a default source root path for the source canonicals
         :param template_persist_path: (optional) a default source root path for the persisted canonicals
         :param template_source_module: (optional) a default module package path for the source handlers
         :param template_persist_module: (optional) a default module package path for the persist handlers
         :param template_source_handler: (optional) a default read only source handler
         :param template_persist_handler: (optional) a default read write persist handler
         :param kwargs: to pass to the connector contract
         :return: the initialised class instance
         """
        pm_file_type = pm_file_type if isinstance(pm_file_type, str) else 'pickle'
        pm_module = pm_module if isinstance(pm_module, str) else 'aistac.handlers.python_handlers'
        pm_handler = pm_handler if isinstance(pm_handler, str) else 'PythonPersistHandler'
        _pm = EventBookPropertyManager(task_name=task_name)
        if not isinstance(template_source_module, str) or template_source_module.startswith('aistac.'):
            template_source_module = 'ds_connectors.handlers.pandas_handlers'
            template_source_handler = 'PandasSourceHandler'
        if not isinstance(template_persist_module, str) or template_persist_module.startswith('aistac.'):
            template_persist_module = 'ds_connectors.handlers.pandas_handlers'
            template_persist_handler = 'PandasPersistHandler'
        super()._init_properties(property_manager=_pm, uri_pm_path=uri_pm_path, pm_file_type=pm_file_type,
                                 pm_module=pm_module, pm_handler=pm_handler, **kwargs)
        super()._add_templates(property_manager=_pm, save=default_save,
                               source_path=template_source_path, persist_path=template_persist_path,
                               source_module=template_source_module, persist_module=template_persist_module,
                               source_handler=template_source_handler, persist_handler=template_persist_handler)
        instance = cls(property_manager=_pm, default_save=default_save)
        instance.modify_connector_from_template(connector_names=instance.pm.connector_contract_list)
        return instance

    @classmethod
    def _from_remote_s3(cls) -> (str, str):
        """ Class Factory Method that builds the connector handlers an Amazon AWS s3 remote store."""
        _module_name = 'ds_connectors.handlers.aws_s3_handlers'
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
    def active_books(self):
        """returns a list of report_portfolio event names"""
        return list(self.__book_portfolio.keys())

    def is_event_book(self, book_name: str) -> bool:
        """Tests if an event book exists and if it is of type AbstractEventBook"""
        if book_name in self.__book_portfolio.keys() and isinstance(self.__book_portfolio.get(book_name),
                                                                    AbstractEventBook):
            return True
        return False

    def get_event_book(self, book_name: str):
        """retrieves an event book instance from the report_portfolio by name"""
        if not self.is_event_book(book_name=book_name):
            raise ValueError(f"The event book instance '{book_name}' can't be found in the report_portfolio.")
        return self.__book_portfolio.get(book_name)

    def start_portfolio(self, exclude_books: [str, list]=None,):
        """runs the intent pipeline

        :param exclude_books: (optional) a list of book_names in the report_portfolio not to start
        """
        portfolio = self.intent_model.run_intent_pipeline(exclude_books=exclude_books)
        self.__book_portfolio.update(portfolio)
        return

    def get_frame_contract(self, book_name: str) -> ConnectorContract:
        """ retrieves a named event book connector

        :param book_name: the unique name of event book
        :return: ConnectorContract
        """
        return self.pm.get_connector_contract(connector_name=book_name)

    def set_frame_contract_template(self, uri_path: str=None, module_name: str=None, handler: str=None,
                               save: bool=None, **kwargs):
        """ sets the book template connector that is used as the base for all event book persistence. for
        parameters not given, the persist connector template is used.

        :param uri_path: a uri path
        :param module_name: a module package name
        :param handler: a handler
        :param save: override of the default save action set at initialisation.
        :param kwargs: additional kwargs
        """
        template = self.pm.get_connector_contract(self.TEMPLATE_PERSIST)
        uri_path = uri_path if isinstance(uri_path, str) else template.uri_raw
        module_name = module_name if isinstance(module_name, str) else template.module_name
        handler = handler if isinstance(handler, str) else template.handler
        if not isinstance(kwargs, dict):
            kwargs = {}
        template.kwargs.update(kwargs)
        book_template = ConnectorContract(uri=uri_path, module_name=module_name, handler=handler, **kwargs)
        if self.pm.has_connector(self.BOOK_TEMPLATE_CONNECTOR):
            self.remove_connector_contract(connector_name=self.BOOK_TEMPLATE_CONNECTOR)
        self.pm.set_connector_contract(connector_name=self.BOOK_TEMPLATE_CONNECTOR, connector_contract=book_template)
        self.pm_persist(save=save)
        return

    def add_frame_contract(self, book_name: str, with_log: bool=None, file_type: str=None, versioned: bool=None,
                           stamped: bool=None, save: bool=None, **kwargs):
        """ adds an event book connector using the book connector template and appending a book pattern to the URI path

        :param book_name: the name of the event book
        :param with_log: (optional) if an events log connector should be created
        :param file_type: (optional) a file type extension. defaults to 'pickle'
        :param versioned: (optional) if the connector uri should be versioned
        :param stamped: (optional) if the connector uri should be timestamped
        :param save: (optional) override of the default save action set at initialisation.
        :param kwargs: extra kwargs to pass to the connector
        """
        if not self.pm.has_connector(connector_name=self.BOOK_TEMPLATE_CONNECTOR):
            raise ConnectionError(f"The book template connector has not been set")
        template = self.pm.get_connector_contract(self.BOOK_TEMPLATE_CONNECTOR)
        uri_file = self.pm.file_pattern(connector_name=book_name, file_type=file_type, versioned=versioned,
                                        stamped=stamped)
        uri = os.path.join(template.path, uri_file)
        if not isinstance(kwargs, dict):
            kwargs = {}
        template.kwargs.update(kwargs)
        cc = ConnectorContract(uri=uri, module_name=template.module_name, handler=template.handler, **kwargs)
        self.add_connector_contract(connector_name=book_name, connector_contract=cc, template_aligned=True, save=save)
        # add the log persist
        if isinstance(with_log, bool) and with_log:
            log = f"{book_name}_log"
            uri_log = self.pm.file_pattern(connector_name=log, file_type=file_type)
            lc = ConnectorContract(uri=uri, module_name=template.module_name, handler=template.handler, **kwargs)
            self.add_connector_contract(connector_name=uri_log, connector_contract=lc, template_aligned=True, save=save)
        return

    def persist_state(self, book_name: str):
        """ persists the current state of an event book"""
        if self.is_event_book(book_name=book_name):
            state = self.current_state(book_name=book_name)[1]
            self.persist_canonical(connector_name=book_name, canonical=state)
        return

    def stop_event_books(self, book_names: [str, list]):
        """stops the event books listed in the book names"""
        book_names = self.pm.list_formatter(book_names)
        for book in book_names:
            if book in self.__book_portfolio.keys():
                self.__book_portfolio.pop(book)
        return

    def reset_portfolio(self):
        """resets the event book report_portfolio removing all running event books and intent"""
        self.__book_portfolio.clear()
        self.pm.reset_intents()
        return

    def remove_event_books(self, book_names: [str, list]):
        """removes the event book"""
        book_names = self.pm.list_formatter(book_names)
        for book in book_names:
            state_name = book
            events_log_name = "_".join([book, '_log'])
            # remove the connectors
            if self.pm.has_connector(state_name):
                self.remove_connector_contract(state_name)
            if self.pm.has_connector(events_log_name):
                self.remove_connector_contract(events_log_name)
            # remove the intent
            self.pm.remove_intent(intent_param=book)
            # remove the report_portfolio entry
            if book in self.__book_portfolio.keys():
                self.__book_portfolio.pop(book)
        return

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

    def report_portfolio(self, stylise: bool=True):
        """ generates a report on all the intent

        :param stylise: returns a stylised dataframe with formatting
        :return: pd.Dataframe
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame.from_dict(data=self.pm.report_intent(), orient='columns')
        df['active'] = df['intent'].isin(list(self.__book_portfolio.keys()))
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
