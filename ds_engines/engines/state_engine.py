import os
from datetime import datetime

import pandas as pd

from ds_engines.managers.state_engine_properties import StateEnginePropertyManager
from ds_foundation.handlers.abstract_handlers import ConnectorContract

__author__ = 'Darryl Oatridge'


class StateEngine(object):

    CONNECTOR_STATE = 'state_connector'
    CONNECTOR_EVENTS = 'events_connector'
    CONNECTOR_PROPERTIES: str

    __state_frame: pd.DataFrame
    __events_log: dict
    __last_persist_time: int
    __event_counter: int
    __last_persist_time: datetime


    def __init__(self, contract_name: str, state_engine_properties: [ConnectorContract]):
        """ Encapsulation class for the state engine manager

        :param contract_name: The name of the properties contract
        :param state_engine_properties: The persist handler for the state engine properties
        """
        if not isinstance(contract_name, str) or len(contract_name) < 1:
            raise ValueError("The contract name must be a valid string")
        self._contract_name = contract_name
        # set property managers
        self._state_pm = StateEnginePropertyManager.from_properties(contract_name=self._contract_name,
                                                                    connector_contract=state_engine_properties)
        self.CONNECTOR_PROPERTIES = self._state_pm.CONTRACT_CONNECTOR
        if self._state_pm.has_persisted_properties():
            self._state_pm.load_properties()
        # initialise the values
        self._state_pm.persist_properties()
        # initialise the globals
        self.__state_frame = pd.DataFrame()
        self.__events_log = dict()
        self.__event_counter = 0
        self.__last_persist_time = datetime.now()

    @classmethod
    def from_remote(cls, contract_name: str=None, location: str=None, path: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers from the default remote.
        This assumes the use of the pandas handler module and pickle persistence on a remote default.

         :param contract_name: (optional) The reference name of the properties contract. Default 'primary_state_engine'
         :param location: (optional) the location or bucket where the data resource can be found. Default 'state_engine'
         :param path: (optional) the path to the persist resources. default 'persist/{contract_name}/'
         :return: the initialised class instance
         """
        _contract_name = contract_name if isinstance(contract_name, str) else 'po1_primary_state'
        _location = 'discovery-vertical' if not isinstance(location, str) else location
        _path = os.path.join('state_engine', 'persist', contract_name) if not isinstance(path, str) else path
        _module_name = 'ds_connectors.handlers.aws_s3_handlers'
        _handler_name = 'AwsS3PersistHandler'
        _properties_resource = os.path.join(_path, 'config_state_engine_{}.pickle'.format(_contract_name))
        _properties_connector = ConnectorContract(resource=_properties_resource, connector_type='pickle',
                                                  location=_location, module_name=_module_name,
                                                  handler='AwsS3PersistHandler')
        rtn_cls = cls(contract_name=_contract_name, state_engine_properties=_properties_connector)
        if not rtn_cls.state_pm.has_connector(cls.CONNECTOR_STATE):
            rtn_cls.set_state_persist_contract(path=_path, location=_location, module_name=_module_name,
                                               handler=_handler_name, **kwargs)
        if not rtn_cls.state_pm.has_connector(cls.CONNECTOR_EVENTS):
            rtn_cls.set_events_persist_contract(path=_path, location=_location, module_name=_module_name,
                                                handler=_handler_name, **kwargs)
        return rtn_cls

    @classmethod
    def from_path(cls, path: str=None, contract_name: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers from the data paths.
        This assumes the use of the pandas handler module.

        :param path: the path persist path
        :param contract_name: (optional) The reference name of the properties contract. Default 'po1_primary_state'
        :return: the initialised class instance
        """
        _contract_name = contract_name if isinstance(contract_name, str) else 'po1_primary_state'
        _path = os.path.join(os.getcwd(), 'persist', _contract_name) if not isinstance(path, str) else path
        _module_name = 'ds_discovery.handlers.pandas_handlers'
        _handler_name = 'PandasPersistHandler'
        _location = os.path.join(_path, _contract_name)
        _properties_connector = ConnectorContract(resource="config_state_engine_{}.pickle".format(_contract_name),
                                                  connector_type='pickle', location=_location, module_name=_module_name,
                                                  handler=_handler_name)
        rtn_cls = cls(contract_name=_contract_name, state_engine_properties=_properties_connector)
        if not rtn_cls.state_pm.has_connector(cls.CONNECTOR_STATE):
            rtn_cls.set_state_persist_contract(path=_path, location=_location, module_name=_module_name,
                                               handler=_handler_name, **kwargs)
        if not rtn_cls.state_pm.has_connector(cls.CONNECTOR_EVENTS):
            rtn_cls.set_events_persist_contract(path=_path, location=_location, module_name=_module_name,
                                                handler=_handler_name, **kwargs)
        return rtn_cls

    @classmethod
    def from_env(cls, contract_name: str=None, **kwargs):
        """ Class Factory Method that builds the connector handlers taking the property contract path from
        the os.envon['PO1_CONTRACT_PATH'] or locally from the current working directory 'po1/contracts' if
        no environment variable is found. This assumes the use of the pandas handler module and yaml persisted file.

         :param contract_name: The reference name of the properties contract
         :return: the initialised class instance
         """
        _path = os.environ['STATE_CONTRACT_PATH'] if 'STATE_CONTRACT_PATH' in os.environ.keys() else None
        return cls.from_path(contract_name=contract_name, path=_path, **kwargs)

    @property
    def contract_name(self) -> str:
        """The contract name of this transition instance"""
        return self._contract_name

    @property
    def version(self) -> str:
        """The version number of the contracts"""
        return self.state_pm.version

    @property
    def temporal_measure(self) -> (str, int):
        """The version number of the contracts"""
        return self.state_pm.temporal_measure

    @property
    def state_pm(self) -> StateEnginePropertyManager:
        """The data properties manager instance"""
        if self._state_pm is None or self._state_pm.contract_name != self.contract_name:
            self._state_pm = StateEnginePropertyManager(self._contract_name)
        return self._state_pm

    def add_event(self, event: pd.DataFrame()) -> datetime:
        _time = datetime.now()
        self.__events_log.update({_time.strftime('%Y%m%d%H%M%S%f'): event})
        self.__state_frame = event.combine_first(self.__state_frame)
        return _time

    def get_current_state(self) -> (datetime, pd.DataFrame):
        return datetime.now(), self.__state_frame.copy(deep=True)

    def persist_state(self, df):
        """Saves the pandas.DataFrame to the perisisted stater"""
        if self.state_pm.has_connector(self.CONNECTOR_STATE):
            handler = self.state_pm.get_connector_handler(self.CONNECTOR_STATE)
            handler.persist_canonical(df)
        return

    def generate_resource_name(self, label: str) -> (str, str):
        """ Returns a persist pattern based on time, contract name, the label and version"""
        _pattern = "{}_{}_{}_{}.pickle"
        _time = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return _pattern.format(str(datetime.now()), self.contract_name, label, self.version), 'pickle'

    def set_state_persist_contract(self, path: str=None, location: str=None, module_name: str=None,
                                   handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param path: (optional) the path to the persist resources. default 'persist/{contract_name}/'
        :param location: (optional) a path, region or uri reference that can be used to identify location of resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        return self._set_persist_contract(connector_name=self.CONNECTOR_STATE, path=path, location=location,
                                          module_name=module_name, handler=handler, **kwargs)

    def set_events_persist_contract(self, path: str=None, location: str=None, module_name: str=None,
                                    handler: str=None, **kwargs):
        """ Sets the persist contract. For parameters not provided the default resource name and data properties
        connector contract module and handler are used.

        :param path: (optional) the path to the persist resources. default 'persist/{contract_name}/'
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
        :param path: (optional) the path to the persist resources. default 'persist/{contract_name}/'
        :param location: (optional) a path, region or uri reference that can be used to identify location of resource
        :param module_name: (optional) a module name with full package path e.g 'ds_discovery.handlers.pandas_handlers
        :param handler: (optional) the name of the Handler Class. Must be
        :param kwargs: (optional) a list of key additional word argument properties associated with the resource
        :return: if load is True, returns a Pandas.DataFrame else None
        """
        if connector_name not in [self.CONNECTOR_STATE, self.CONNECTOR_EVENTS]:
            raise ValueError("The connector name must be either {} or {}. passed {}".format(self.CONNECTOR_STATE,
                                                                                            self.CONNECTOR_EVENTS,
                                                                                            connector_name))
        reference = connector_name if self.state_pm.has_connector(connector_name) else self.CONNECTOR_PROPERTIES
        label = 'state' if connector_name == self.CONNECTOR_STATE else 'events'
        path = os.path.join('persist', self.contract_name) if not isinstance(path, str) else path
        name, connector_type = self.generate_resource_name(label=label)
        resource = os.path.join(path, name)
        if not isinstance(location, str):
            location = self.state_pm.get_connector_contract(reference).location
        if not isinstance(module_name, str):
            module_name = self.state_pm.get_connector_contract(reference).module_name
        if not isinstance(handler, str):
            handler = self.state_pm.get_connector_contract(reference).handler
        # remove the connector and handler
        if self.state_pm.has_connector(connector_name):
            self.state_pm.remove_connector_contract(connector_name)
        self.state_pm.set_connector_contract(connector_name, resource=resource, connector_type=connector_type,
                                             location=location, module_name=module_name, handler=handler, **kwargs)
        self.state_pm.persist_properties()
        return

    def load_state_canonical(self) -> pd.DataFrame:
        """loads the clean pandas.DataFrame from the clean folder for this contract"""
        if self.state_pm.has_connector(self.CONNECTOR_STATE):
            handler = self.state_pm.get_connector_handler(self.CONNECTOR_STATE)
            df = handler.load_canonical()
            return df
        return pd.DataFrame()

    def load_events_canonical(self) -> pd.DataFrame:
        """loads the clean pandas.DataFrame from the clean folder for this contract"""
        if self.state_pm.has_connector(self.CONNECTOR_EVENTS):
            handler = self.state_pm.get_connector_handler(self.CONNECTOR_EVENTS)
            df = handler.load_canonical()
            return df
        return pd.DataFrame()

    def report_connectors(self, stylise: bool=True):
        """ generates a report on the source contract

        :param stylise: (optional) returns a stylised dataframe with formatting
        :return: pd.DataFrame
        """
        stylise = True if not isinstance(stylise, bool) else stylise
        style = [{'selector': 'th', 'props': [('font-size', "120%"), ("text-align", "center")]},
                 {'selector': '.row_heading, .blank', 'props': [('display', 'none;')]}]
        df = pd.DataFrame()
        join = self.state_pm.join
        dpm = self.state_pm
        df['param'] = ['connector_name', 'resource', 'connector_type', 'location', 'module_name',
                       'handler', 'modified', 'kwargs']
        for name_key in dpm.get(join(dpm.KEY.connectors_key)).keys():
            connector_contract = dpm.get_connector_contract(name_key)
            if isinstance(connector_contract, ConnectorContract):
                if name_key == self.CONNECTOR_EVENTS:
                    label = 'Events Source'
                elif name_key == self.CONNECTOR_STATE:
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
        self.state_pm.set_version(version=version)
        self.state_pm.persist_properties()
        return

    def set_temporal_measure(self, measure: str, value: int):
        """ sets the temporal measure of when to persist state. The supported types are 'count' and 'time'.
        both require an in value, count represented by the number of events, time represented by the number of seconds
        :param measure: the measure type of the temporal gaps. The supported types are 'count' and 'time'
        :param value: the representative unit value of the rhythm, number of events or seconds.
        """
        self.state_pm.set_temporal_rhythm(measure=measure, value=value)
        self.state_pm.persist_properties()
        return

