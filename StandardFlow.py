import luigi
import luigi.format

class DependentTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./data/Standard/out.txt', format=luigi.format.UTF8)

    def run(self):
        with self.output().open('w') as fout:
            fout.write('標準的なLuigiの出力結果')

class StartTask(luigi.WrapperTask):
    def requires(self):
        return DependentTask()

if __name__ == '__main__':
    luigi.run(main_task_cls=StartTask)