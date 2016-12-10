from threading import Thread, Condition, RLock, Lock
from xmlrpc import server as xmlrpcserver, client as xmlrpcclient
from collections import namedtuple
from os import getcwd
import runpy
import queue
import asyncio

#package imports
import drone.tasksupport
import hivenode
#

# test
runpy.run_path(getcwd()+r"\drone\Volatile\TestTask\vector_mult.py")
#

AwaitedTask = namedtuple('AwaitedTask', ['task', 'condition'])

class Drone(hivenode.HiveNode):
    
    _maxattempts = 10

    # initialization
    def __init__(self, host_port : int, server_addr : (str, int) = None):
        super().__init__(host_port,enterpoint = server_addr,broadcastport = 4000)

        self._task_count = 0
        self._task_count_lock = RLock()
        self._task_awaited = {}
        self._task_awaited_lock = RLock()

        self._exec_task_queue = queue.Queue()
        self._exec_lock = Condition()

        tasksupport._drone = self

        self._drone_name = None

    def _register_server(self):
        super().rpc_server.register_function(self.Hive_PutTask)
        super().rpc_server.register_function(self.Hive_Hello)
        super().rpc_server.register_function(self.Hive_TaskResult)
        

    #RPC my end

    def Hive_PutTask(t) -> NoneType:
        pass

    def Hive_Hello(a : (str, int)) -> NoneType:
        pass

    def Hive_TaskResult(t_id : int, d : str) -> NoneType:
        
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

    def _Hive_overlord_GetTask(a : (str, int)) -> NoneType:
        pass

    def _Hive_overlord_Hello(a : (str, int)) -> NoneType:
        pass

    def _Hive_overlord_Register(a : (str, int)) -> (aa,ao,asc):
        pass

    def _Hive_drone_TaskResult(owner, t_id : int, d : str) -> NoneType:
        try:
            d = super().get_proxy(owner)
            d.Hive_TaskResult(taskid, result)
        finally:
            #TODO: reliability
            print("error debug!!")

    #methods

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

    @asyncio.coroutine
    def await_task(self, task_id : int):
        with self._task_awaited_lock:
            if not task_id in self._task_awaited:
                raise Exception("invalid task id")
            task = self._task_awaited[task_id]
        
        current_task = tasksupport.CurrentTask
        self._exec_lock.release()
        with task.condition:
            while not task.task.done:
                task.condition.wait()

            self._exec_lock.acquire()
            tasksupport.CurrentTask = current_task

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