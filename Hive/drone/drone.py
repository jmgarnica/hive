from threading import Thread, Condition, RLock, Lock
from xmlrpc import server as xmlrpcserver, client as xmlrpcclient
from collections import namedtuple
import runpy
import queue
import tasksupport
import asyncio

#TODO: ultra mega test zone
#
from os import getcwd 
runpy.run_path(getcwd()+r"\drone\Volatile\TestTask\vector_mult.py")
#

# Name convention
# _<name> private methods
# [_]Hive[_(receive|send)]_<Name> implementation of <Name> Hive protocol
# action, if marked as "receive" or "send" the action is asynchronous, 
# the drone starts using "send" and it returns immediately True if the 
# comunication succeed, false otherwise, then via rpc, the actual reponse
# is invoked in the "receive" method
# only "receive" and "send" with perfectly matching <Name> are peers, 
# only "receive" part must be register on the rpc server

AwaitedTask = namedtuple('AwaitedTask', ['task', 'condition'])

class Drone:
    
    _maxattempts = 10

    # initialization
    def __init__(self, host_port : int, server_addr : (str, int) = None):
        self._task_count = 0
        self._task_count_lock = RLock()
        self._task_awaited = {}
        self._task_awaited_lock = RLock()

        self._exec_task_queue = queue.Queue()
        self._exec_lock = Condition()

        tasksupport._drone = self

        self._host_port = host_port
        self._register_server()

        self._drone_name = None
        self._connect(server_addr)

        self._propagate_address()
        self._rpc_server_daemon.start()

    #connectivity

    def _connect(self, server_addr : (str, int)) -> None:
        server = self._Hive_Hello(server_addr if server_addr != None else Drone._locate_server())

        register = self._Hive_Register(server, self._drone_name)
        self._ring_connections = register[:3]
        if self._drone_name == None:
            self._drone_name = register[3]

    def _propagate_address(self):
        '''an infinite loop lestening for overlord address udp requests'''

        def propagation_loop():
            #TODO: implement
            pass

        self._propagate_address_thread = Thread(target = propagation_loop)
        self._propagate_address_thread.start()

    def _register_server(self):
        self._rpcserver = xmlrpcserver.SimpleXMLRPCServer(("localhost",self._host_port))
        self._rpcserver.register_function(self.Hive_TaskResult)
        self._rpcserver.register_function(self.Hive_receive_TaskRequest)
        

        self._rpc_server_daemon = Thread(target = self._rpcserver.serve_forever)
        self._rpc_server_daemon.daemon = True

    @staticmethod
    def _locate_server() -> (str, int):
        '''this should return the addres of a Overlord in the subnet'''
        #TODO: locate_server
        pass

    @staticmethod
    def _get_proxy(addr : (str, int)):
        #TODO: check reliability
        return xmlrpcclient.ServerProxy("http://{0}:{1}/".format(addr[0],addr[1]))

    # processing

    @property
    def rpc_listen_point(self) -> (str, int):
        #TODO: rpc_listen_point
        pass

    def addtask(self, task : str, data : str,  dependencies : list = None) -> bool:
        with self._task_count_lock:
            self._task_count += 1
            t_id = self._task_count

        a = AwaitedTask(tasksupport.Task(task, data, owneraddr = self.rpc_listen_point, taskid = t_id), Condition())
        with self._task_awaited_lock:
            self._task_awaited[t_id] = a

        return True

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

    def start_work(self):

        def taskthread(task):
            self._exec_lock.acquire()
            tasksupport.CurrentTask = task

            #TODO: put the actual path here
            runpy.run_path(task.name)
            if not task.done:
                print("task ", task.name, " exit without a result")
                      
            self._Hive_TaskResponse(task.owner, task.task_id, task.result)

            tasksupport.CurrentTask = None
            self._exec_lock.release()

        def workloop():
            while True:
                with self._exec_lock:
                    self._Hive_send_TaskRequest()
                    while self._exec_task_queue.qsize() == 0:
                        self._exec_lock.wait()
                    Thread(target = taskthread, args =(self._exec_task_queue.get(),)).start()              

        self._work_thread = Thread(target = workloop)
        self._work_thread.start()






    # Hive: Drone - Overlord
    def _Hive_Hello(self, server : (str, int)) -> (str,int):
        #TODO: _Hive_Hello
        pass

    def _Hive_Register(self, server : (str, int), drone_name : str = None) -> ((str, int),(str, int), (str,int), str):
        #TODO: _Hive_Register
        pass

    def _Hive_send_TaskRequest(self) -> None:
        #TODO: _Hive_send_TaskRequest
        pass

    def Hive_receive_TaskRequest(self, task : {}) -> None:
        
        pass





    # Hive: Drone - Drone
    def _Hive_TaskResponse(self, drone_addr : (str, int),  taskid : int, result : str) -> None:
        drone = Drone._get_proxy(drone_addr)
        try:
            drone.Hive_TaskResult(taskid, result)
        finally:
            #TODO: reliability
            print("error debug!!")

    def Hive_TaskResult(self, task_id : int, data : str) -> None:

        def task_result_worker():

            with self._task_awaited_lock:
                if not (task_id in self._task_awaited):
                    return
                
                task = self._task_awaited[task_id]

                with task.condition:
                    task.task.finalize(data)
                    task.condition.notify()
        
        Thread(target = task_result_worker).start()

