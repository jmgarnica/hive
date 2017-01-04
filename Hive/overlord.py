from threading import Thread, Condition, RLock, Lock
from xmlrpc import server as xmlrpcserver, client as xmlrpcclient
from collections import namedtuple
from os import getcwd
import runpy
import queue
#import asyncio
import hivenode

class Overlord(hivenode.HiveNode):

    def __init__(self, rpc_port, enterpoint = None, broadcastport = None):
        return super().__init__(rpc_port, enterpoint = enterpoint, broadcastport = broadcastport)

    #RPC may end

    def Hive_EntreRing(self, a : (str, int)) -> ((str,int),str):
        pass

    def Hive_SetPredecesor(self, a : (str, int)) -> NoneType:
        pass

    def Hive_Address(self, a : (str, int), i : int) -> NoneType:
        pass

    def Hive_Who(self, c : int, a : (str, int), i : int) -> NoneType:
        pass

    def Hive_Replicate(self, t : {}, sf : int, sc : int, rc : int) -> NoneType:
        pass

    def Hive_Consume(self, a : (str, int)) -> NoneType:
        pass

    def Hive_PutTask(self, t : {}) -> NoneType:
        pass
    
    def Hive_GetTask(self, a : (str, int)) -> NoneType:
        pass
    
    def Hive_Hello(self, a : (str, int)) -> NoneType:
        pass
    
    def Hive_Register(self, a : (str, int)) -> (aa, ao, asc):
        pass 

    #RPC other end

    def _Hive_overlord_EntreRing(self, target, myaddrss) -> ((str,int),str):
        pass

    def _Hive_SetPredecesor(self, target, a) -> None:
        pass