from threading import Thread, Condition, RLock, Lock
from pathlib import Path

import runpy
import queue

import tasksupport
import hivenode


class Drone(hivenode.HiveNode):
    
    _maxattempts = 10

    # initialization
    def __init__(self, host_port : int, server_addr : (str, int) = None):
        super().__init__(host_port, broadcastport= 4000)

        self._task_count = 0
        self._task_count_lock = RLock()
        self._task_awaited = {}
        self._task_awaited_lock = RLock()

        self._exec_task_queue = queue.Queue()
        self._exec_lock = Condition()

        self._drone_name = None

        self._register_server()

        self._hello = None
        self._register = None

        self.server = hivenode.ServerInfo(server_addr)

        self._tasks = {}

    def _register_server(self):
        super().rpc_server.register_function(self.Hive_PutTask)
        super().rpc_server.register_function(self.Hive_Hello)
        super().rpc_server.register_function(self.Hive_TaskResult)

    #RPC my end

    def Hive_PutTask(self, a, t) -> NoneType:
        
        def put():
            with self._exec_lock:
                self._exec_task_queue.put(hivenode.Task(**t))
                self._exec_lock.notify()
        
        Thread(target= put, daemon= True).start()

    def Hive_Hello(self, a : (str, int), server: (str, int)) -> NoneType:

        def hello():
            with self.server.hello_condition:
                if not self.server.hello:
                    self.server.hello = server
                    self.server.hello_condition.notify()

        Thread(target= hello, daemon= True).start()
    
    def Hive_Register(self, a: (str, int), p: (str, int), s: (str, int), name: str):

        def register():
            with self.server.register_condition:
                if not self.server.address:
                    self.server.address = a
                    self.server.predecessor = p
                    self.server.successor = s

                    if not self._drone_name:
                        self._drone_name = name

                    self.server.register_condition.notify()

        Thread(target= register, daemon= True).start()

    def Hive_TaskResult(self, a, t_id : int, d : str) -> NoneType:
        
        def task_result_worker():

            with self._task_awaited_lock:
                if not (t_id in self._task_awaited):
                    return
                
                task = self._task_awaited[t_id]

                with task.condition:
                    task.task.result = d
                    task.task.done = True
                    task.condition.notify()
                     
        Thread(target= task_result_worker, daemon= True).start()


    #RPC target end

    def _Hive_overlord_PutTask(self, t) -> NoneType:
        self.get_server_proxy().Hive_PutTask(t)

    def _Hive_overlord_GetTask(self) -> NoneType:
        self.get_server_proxy().Hive_GetTask()   

    def _Hive_drone_TaskResult(self, owner, t_id : int, d : str) -> NoneType:
        try:
            d = self.get_proxy(owner)
            d.Hive_TaskResult(taskid, result)
        finally:
            print("error debug!!")

    #methods

    def get_server_proxy(self):
        with self.server.lock:
            stage = 6 if self.server.connected else 1            
            server = None
            proxy = None

            while not server:
                if not stage in [ 1, 2, 4, 6]:
                    self.server.connected = False
                    raise ConnectionError()

                if stage == 1:
                    try:
                        proxy = self.get_proxy(self.server.enter_address)
                        stage = 2
                        counter = self.max_attempts
                    except:
                        if self.server.enter_address:
                            self.server.enter_address = None
                        else:
                            stage = 0

                if stage == 2:
                    self.server.hello = None
                    waittime = self.time_to_wait*(self.max_attempts - counter - 1)
                    try:
                        proxy.Hive_Hello()
                        with self.server.hello_condition:
                            stage = 3
                            self.server.hello_condition.wait(waittime)
                            if not self.server.hello:
                                stage = 2
                                counter -= 1
                            else:
                                proxy = self.get_proxy(self.server.hello)
                                stage = 4
                                counter = self.max_attempts
                    except:
                        stage = 2
                        counter -= 1
                        if counter == 0:
                            stage = 1
                
                if stage == 4:
                    self.server.address = None
                    self.server.predecessor = None
                    self.server.successor = None
                    waittime = self.time_to_wait*(self.max_attempts - counter - 1)
                    try:
                        proxy.Hive_Register()
                        with self.server.register_condition:
                            stage = 5
                            self.server.register_condition.wait(waittime)
                            if not self.server.address:
                                stage = 4
                                counter -= 1
                            else:
                                stage = 6
                    except:
                        stage = 4
                        counter -= 1
                        if counter == 0:
                            stage = 2

                if stage == 6:
                    if proxy:
                        server = proxy
                    else:
                        try:
                            server = get_proxy(self.server.predecessor)
                        except:
                            try:
                                server = get_proxy(self.server.successor)
                            except:
                                server = None
                                stage = 2
            
            self.server.connected = True

            return server

    def start_work(self):

        from time import sleep

        def taskthread(task):
            with self._exec_lock:
                tasksupport.TaskData.drone = self
                tasksupport.TaskData.task = task

                with self._task_count_lock:
                    self._task_count += 1
                    t_id = self._task_count

                p = Path("./Volatile/{0}".format(t_id))
                if not p.exists():
                    p.mkdir(parents= True)

                for d in task._dependencies:
                    q = p / (d[0] + ".py")
                    with q.open(mode= 'w') as f:
                        f.write(d[1].code)
                        
                    d[1].path = str(q)

                runpy.run_path(task._dependencies[task.name].path)               
                      
                self._Hive_drone_TaskResult(task.owner, task.task_id, task.result)

                tasksupport.TaskData.task = None

        def workloop():
            while True:
                with self._exec_lock:
                    self._Hive_overlord_GetTask()
                    while self._exec_task_queue.qsize() == 0:
                        self._exec_lock.wait()
                    Thread(target = taskthread, args =(self._exec_task_queue.get(),)).start()
                
                sleep(0.5)

        self._work_thread = Thread(target = workloop)
        self._work_thread.start()

    async def await_task(self, task_id : int):
        with self._task_awaited_lock:
            if not task_id in self._task_awaited:
                raise Exception("invalid task id")
            task = self._task_awaited[task_id]
        
        self._exec_lock.release()
        with task.condition:
            while not task.task.done:
                task.condition.wait()

            self._exec_lock.acquire()           

            with self._task_awaited_lock:
                del self._task_awaited[task_id]           
            return task.task.resutl

    def addtask(self, task : str, data : str,  dependencies : list = None, parent_id = None):
        with self._task_count_lock:
            self._task_count += 1
            t_id = self._task_count
        
        with self._task_awaited_lock:
            dep = self._task_awaited[parent_id]._dependencies if parent_id else self._tasks
            p = {d: dep[d] for d in dependencies}
            p.update({task: dep[task]})

            t = hivenode.Task(name= task, data= data, id = t_id, dependencies= p)
            a = hivenode.AwaitedTask(t, Condition())
            self._task_awaited[t_id] = a

        i = 0
        while i < self.max_attempts:
            try:
                self._Hive_overlord_PutTask(t)
                return t_id
            except:
                i += 1

    def loadtask(self, path):
        
        with open(path) as f:
            code = f.read()

        dep = hivenode.Dependency(code, path)
        self._tasks.update({Path(path).name: dep})
