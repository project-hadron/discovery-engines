from ds_engines import Controller

__author__ = 'Darryl Oatridge'


def domain_swarm():
    swarm = Controller.from_env(default_save=False, has_contract=True)
    swarm.run_pipeline()


if __name__ == '__main__':
    domain_swarm()
