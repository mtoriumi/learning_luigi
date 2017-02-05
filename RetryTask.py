import luigi
import luigi.format
import datetime


class RetryedTask(luigi.Task):

    retry = 0

    retry_count = 2

    def output(self):
        return luigi.LocalTarget('./data/Retry/out.txt', format=luigi.format.UTF8)

    def run(self):
        print(datetime.datetime.now())
        raise ValueError
        with self.output().open('w') as fout:
            fout.write('標準的なLuigiの出力結果')

    def on_failure(self, exception):
        print(exception)


class StartTask(luigi.Task):

    def output(self):
        return luigi.LocalTarget('./data/Retry/StartTaskout.txt', format=luigi.format.UTF8)

    def run(self):
        yield [RetryedTask()]
        with self.output().open('w') as fout:
            fout.write('標準的なLuigiの出力結果')


if __name__ == '__main__':
    luigi.run(main_task_cls=RetryedTask)