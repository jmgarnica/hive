
class Task:

    def __init__(self, name: str, data: str, owneraddr: (str, int), taskid: int, dependencies: [], path: str):
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
        return self._done

    @property
    def owner(self):
        return self._owneraddr

    def finalize(self, result):
        self.result = result
        self.done = True