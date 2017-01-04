from threading import Thread, Condition, RLock, Lock
from collections import namedtuple
from os import getcwd
import runpy
import queue
#import asyncio

#package imports
import tasksupport
import hivenode
#

# test
runpy.run_path(getcwd()+r"\Volatile\TestTask\vector_mult.py")
#

AwaitedTask = namedtuple('AwaitedTask', ['task', 'condition'])
ServerInfo = namedtuple('ServerInfo', ['address',
                                       'predecessor',
                                       'successor',
                                       'connected',
                                       'lock',
                                       'enter_address',
                                       'hello', 'hello_condition'
                                       'register_condition'])

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

        tasksupport._drone = self

        self._drone_name = None

        self._register_server()

        self._hello = None
        self._register = None

        self.server = ServerInfo(None, None, None, False,
                                 RLock(), server_addr,
                                 None, Condition(),
                                 Condition())



    def _register_server(self):
        super().rpc_server.register_function(self.Hive_PutTask)
        super().rpc_server.register_function(self.Hive_Hello)
        super().rpc_server.register_function(self.Hive_TaskResult)
        

    #RPC my end

    def Hive_PutTask(self, a, t) -> NoneType:
        pass

    def Hive_Hello(self, a : (str, int), server: (str, int)) -> NoneType:

        def hello():
            with self.server.hello_condition:
                if not self.server.hello:
                    self.server._replace(hello= server)

                    self.server.hello_condition.notify()

        Thread(target= hello, daemon= True).start()
    
    def Hive_Register(self, a: (str, int), p: (str, int), s: (str, int), name: str):

        def register():
            with self.server.register_condition:
                if not self.server.address:
                    self.server._replace(address= a)
                    self.server._replace(predecessor= p)
                    self.server._replace(successor= s)

                    if not self._drone_name:
                        self._drone_name = name

                    self.server.register_condition.notify()

        Thread(target= register, daemon= True).start()

    def Hive_TaskResult(self, a, t_id : int, d : str) -> NoneType:
        
        def task_result_worker():

            with self._task_awaited_lock:
                if not (task_id in self._task_awaited):
                    return
                
                task = self._task_awaited[task_id]

                with task.condition:
                    task.task.finalize(data)
                    task.condition.notify()
                     
        Thread(target = task_result_worker).start()


    #RPC target end

    def _Hive_overlord_PutTask(server, t) -> NoneType:
        super().get_proxy(server).Hive_PutTask(t)
        pass

    def _Hive_overlord_GetTask() -> NoneType:
        pass   

    def _Hive_drone_TaskResult(owner, t_id : int, d : str) -> NoneType:
        try:
            d = super().get_proxy(owner)
            d.Hive_TaskResult(taskid, result)
        finally:
            #TODO: reliability
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
                            self.server._replace(enter_address = None)
                        else:
                            stage = 0

                if stage == 2:
                    self.server._replace(hello= None)
                    waittime = self.time_to_wait*(self.max_attempts - counter - 1)
                    try:
                        proxy.Hive_Hello()
                        with self.server.hello_condition:
                            stage = 3
                            self.server.hello_condition.wait(waittime) #make sure that this does not returns inmediately
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
                    self.server._replace(address= None)
                    self.server._replace(predecessor= None)
                    self.server._replace(successor= None)
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
            
            return server

    def start_work(self):

        def taskthread(task):
            self._exec_lock.acquire()
            tasksupport.CurrentTask = task

            #TODO: put the actual path here
            runpy.run_path(task.name)
            if not task.done:
                print("task ", task.name, " exit without a result")
                      
            self._Hive_drone_TaskResult(task.owner, task.task_id, task.result)

            tasksupport.CurrentTask = None
            self._exec_lock.release()

        def workloop():
            while True:
                with self._exec_lock:
                    self._Hive_overlord_GetTask(super().find_my_ip())
                    while self._exec_task_queue.qsize() == 0:
                        self._exec_lock.wait()
                    Thread(target = taskthread, args =(self._exec_task_queue.get(),)).start()              

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

    def addtask(self, task : str, data : str,  dependencies : list = None) -> bool:
        with self._task_count_lock:
            self._task_count += 1
            t_id = self._task_count

        a = AwaitedTask(tasksupport.Task(task, data, owneraddr = self.rpc_listen_point, taskid = t_id), Condition())
        with self._task_awaited_lock:
            self._task_awaited[t_id] = a

        return True