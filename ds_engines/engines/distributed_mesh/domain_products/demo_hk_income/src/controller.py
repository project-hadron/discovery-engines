from ds_engines import Controller

__author__ = 'Darryl Oatridge'


def domain_controller():
    controller = Controller.from_env(default_save=False, has_contract=True)
    controller.run_pipeline()


if __name__ == '__main__':
    domain_controller()
