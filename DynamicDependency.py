import luigi
import luigi.format
import os.path

class SuccessTask(luigi.Task):
    """
    普通に出力成功するタスク
    """
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/SuccessTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
        with self.output().open('w') as fout:
            fout.write('動的なLuigiの正常な出力ファイル')

class OpenNopTask(luigi.Task):
    """
    Openするけど何も出力しないタスク（空ファイルが作られる）
    """
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/OpenNopTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
        self.output().open('w').close()

class NopTask(luigi.Task):
    """
    Openすらしないタスク（ファイルが作られないので無限ループ）
    """
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/NopTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
        pass

class ExceptionTask(luigi.Task):
    """
    例外で処理中断するタスク（例外が起きた場合は後続処理復帰不可）
    """
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/ExceptionTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
        with self.output().open('w') as fout:
            fout.write('なんか出力')
        raise ValueError("error!")

class  ErrorHandlingTask(luigi.Task):
    """
    なにかしらエラーが起きても処理を継続させたい場合は自らハンドリングが必要
    少なくとも、何かしらのファイルが返るか、例外が発生しないとタスクが無限ループするため、
    処理を継続させたい場合には、なにかしらのファイルの返却を要する
    """
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/ErrorHandlingTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
        if 2 == 3 + 1: # 問題発生
            with self.output().open('w') as fout:
                fout.write('処理成功時の正常データ')
        else: # 空ファイルをエラー扱いとするしかない？
            self.output().open('w').close()

class StartTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./data/Dynamic/StartTaskOut.txt', format=luigi.format.UTF8)

    def run(self):
#        success_task_target   = yield SuccessTask()
#        open_nop_task_target  = yield OpenNopTask()    # 空ファイルが作られる
#        nop_task_target       = yield NopTask()        # 出力ファイルが作られないので永遠に復帰しない
#        exception_task_target = yield ExceptionTask()　# 例外起きたら復帰不可能
        tasks = (SuccessTask(), OpenNopTask(), ErrorHandlingTask())
        task_targets = yield tasks
        results = zip(tasks, task_targets)

        with self.output().open('w') as fout:
#            fout.write('success_task_target:{}\n'.format(str(success_task_target)))
#            fout.write('open_nop_task_target:{}\n'.format(str(open_nop_task_target)))
#            fout.write('nop_task_target:{}\n'.format(str(nop_task_target)))
#            fout.write('exception_task_target:{}\n'.format(str(exception_task_target)))

            # 全部ファイルは返ってくるので、空ファイルかどうか判定し、空ならエラー扱いとする
            for result in results:
                taskname = str(result[0])
                if os.path.getsize(result[1].path):
                    fout.write('{} is success\n'.format(taskname))
                else:
                    fout.write('{} is failed\n'.format(str(result[0])))

if __name__ == '__main__':
    if not luigi.run(main_task_cls=StartTask):
        import sys
        sys.exit(1)