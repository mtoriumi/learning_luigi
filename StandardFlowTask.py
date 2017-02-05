from luigi import Task, run
from luigi.mock import MockTarget
from inspect import currentframe


class DependentTask(Task):

    def output(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        return MockTarget('out.txt')

    def run(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        with self.output().open('w') as fout:
            fout.write('DependentTask is succeeded')

    def on_success(self):
        print("Reached {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))

    def on_failure(self, exception):
        print("Reached {}.{} {}".format(self.__class__.__name__, currentframe().f_code.co_name, exception))


class StartTask(Task):

    def output(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        return MockTarget("StartTaskOut.txt")

    def requires(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        return DependentTask()

    def run(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        with self.output().open('w') as fout:
            fout.write('StartTask is succeeded')

    def on_success(self):
        print("Reached {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))

    def on_failure(self, exception):
        print("Reached {}.{} {}".format(self.__class__.__name__, currentframe().f_code.co_name, exception))



if __name__ == '__main__':
    run(main_task_cls=StartTask)