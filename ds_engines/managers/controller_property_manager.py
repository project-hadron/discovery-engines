from aistac.properties.abstract_properties import AbstractPropertyManager

__author__ = 'Darryl Oatridge'


class ControllerPropertyManager(AbstractPropertyManager):

    DEFAULT_INTENT_LEVEL = 'primary_intent'

    def __init__(self, task_name: str, username: str):
        """Abstract Class for the Master Properties"""
        root_keys = [{'use_case': ['title', 'domain', 'project', 'overview', 'scope', 'situation', 'opportunity',
                                   'actions', 'author']}]
        knowledge_keys = []
        super().__init__(task_name=task_name, root_keys=root_keys, knowledge_keys=knowledge_keys, username=username)

    @property
    def use_case(self) -> dict:
        """Return the use case"""
        return self.get(self.KEY.use_case_key, {})

    def report_use_case(self) -> dict:
        """Return the use case report"""
        report = dict()
        for catalog in self.get(self.KEY.use_case_key, {}).keys():
            _key = self.join(self.KEY.use_case_key, catalog)
            report[catalog] = self.get(_key, '')
        return report

    def set_use_case(self, title: str=None, domain: str=None, project: str=None, overview: str=None, scope: str=None,
                     situation: str=None, opportunity: str=None, actions: str=None, license_type: str=None,
                     license_name: str=None, license_uri: str=None, provider_name: str=None, provider_uri: str=None,
                     provider_note: str=None, author_name: str=None, author_contact: str=None):
        """ sets the use_case values. Only sets those passed

        :param title: (optional) the title of the use_case
        :param domain: (optional) the domain it sits within
        :param project: (optional) the project name the use case is part of
        :param overview: (optional) a overview of the use case
        :param scope: (optional) the scope of responsibility
        :param situation: (optional) The inferred 'Why', 'What' or 'How' and predicted 'therefore can we'
        :param opportunity: (optional) The opportunity of the situation
        :param actions: (optional) the actions to fulfil the opportunity
        :param author: (optional) the author of the use case
        """
        params = locals().copy()
        params.pop('self', None)
        for name, value in params.items():
            if value is None:
                continue
            if name in ['title', 'domain', 'description', 'situation', 'opportunity', 'actions', 'author']:
                self.set(self.join(self.KEY.use_case_key, name), value)
        return

    def reset_use_case(self):
        """resets use case back to its default values"""
        self._base_pm.remove(self.KEY.use_case_key)
        self.set(self.KEY.use_case_key, {})
        return
