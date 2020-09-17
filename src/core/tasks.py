from prefect import Task


class DataTask(Task):
    """ Wrapper of Task to write data by default.

    In Task on Perfect, we always need to set checkpoint= True
    to persist output.

    Note:
        - We use this class to change location name depending on the return of Task.run.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.checkpoint = True
        # self.result = LocalResult()
        # self.target = 'sample.pickle'
        # self.cache_for = datetime.timedelta(days=1)
        # self.cache_validator = prefect.engine.cache_validators.all_parameters
