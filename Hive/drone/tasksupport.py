from asyncio import coroutine

class Task:

    def __init__(self, name : str, data : str, *, owneraddr : (str, int), taskid : int):
        self._name = name
        self._data = data
        self._owneraddr = owneraddr
        self._taskid = taskid
        self._result = None
        self._done = False

    @property
    def task_id(self):
        return self._taskid

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data
    
    @property
    def result(self) -> str:
        return self._result

    @result.setter
    def set_result(self, result : str) -> None:
        self._result = result

    @property
    def done(self):
        return self._done;
    @property
    def owner(self):
        return self._owneraddr

    def finalize(self, result):
        self.result = result
        self.done = True

#meter esto en local
CurrentTask = None
_drone = None

@coroutine
def distributed_process(task : str, data : str,  dependencies : list = None):
    if _drone == None:
        raise Exception("no drone attached")
    task = _drone.addtask(task, data, dependencies)
    
    if task == None:
        raise Exception("could not start the task")
    

    return (yield from _drone.await_task(task))
