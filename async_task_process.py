from task_dag_executor import AbstractTask, DagTaskNode
from py_active_objects.active_objects import ActiveObjectsController, AbstractTask
import asyncio

class AsyncTaskProcess(AbstractTask):

    def __init__(self, controller:ActiveObjectsController, task_func=None):
        self.task_func = task_func
        self.task = asyncio.create_task(self._do_task())
        self._cancel_async_task = True
        super().__init__(controller)

    async def _do_task(self):
        try:
            exit_code = await self.do_task()
            if self._cancel_async_task:
                self._cancel_requested = False
            self.set_exit_code(exit_code if exit_code is not None else -1)
        except Exception as e:
            self.error = e
            self.set_exit_code(-1)
        self.completed_signal.signalAll()
        self.controller.wakeup()

    async def do_task(self)->int:
        return await self.task_func()

    def cancel_async_task(self, kill:bool):
        if self.task.cancel("Killed" if kill else "Canceled"):
            self.set_exit_code(-1)
            self.completed_signal.signalAll()

    def cancel(self, kill:bool=False):
        super().cancel(kill)
        if self._cancel_async_task:
            self.cancel_async_task(kill)

    def close(self):
        self.task.cancel()
        self.task = None
        super().close()

def test_process(task:DagTaskNode):

    async def test():
        await asyncio.sleep(1)
        return 0

    return AsyncTaskProcess(task.controller, test)

class SystemTaskProcess(AsyncTaskProcess):

    def __init__(self, controller:ActiveObjectsController, commands:list, cwd=None):
        super().__init__(controller)
        self.commands=commands
        self.cwd=cwd
        self.proc = None
        self._cancel_async_task = False

    async def do_task(self)->int:
        self.proc = await asyncio.create_subprocess_exec(
            self.commands[0],
            *(self.commands[1:]),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=self.cwd
        )
        return await self.proc.wait()

    def cancel(self, kill:bool=False):
        super().cancel(kill)
        if self.proc is not None:
            if kill:                
                self.proc.kill()
            else:
                self.proc.terminate()
        else:
            super().cancel_async_task(kill)

    def close(self):
        self.proc = None
        super().close()
