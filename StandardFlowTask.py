from luigi import Task, WrapperTask, run
from luigi.format import UTF8
from luigi.mock import MockTarget
from inspect import currentframe


class DependentTask(Task):

    def output(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        return MockTarget('out.txt', format=UTF8)

    def run(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        with self.output().open('w') as fout:
            fout.write('DependentTask is Succeeded')


class StartTask(WrapperTask):

    def requires(self):
        print("running {}.{}".format(self.__class__.__name__, currentframe().f_code.co_name))
        return DependentTask()


if __name__ == '__main__':
    run(main_task_cls=StartTask)