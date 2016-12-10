import drone.tasksupport as tasksupport
import asyncio

def vector_serializer(vector : (int, list, list)) -> str:
    return "{0}|{1}|{2}".format(vector[0],
                                " ".join(vector[1]),
                                " ".join(vector[2]))

def vector_deserializer(vector : str) -> (int, list, list):
    struct = vector.split('|')
    return (int(struct[0]),
           [int(i) for i in vector[1].split()],
           [int(i) for i in vector[2].split()])

split_threshold, vector_a, vector_b = vector_deserializer(tasksupport.CurrentTask.data)

if len(vector_a) != len(vector_b):
    raise Exception("vectors length mismatch")

@asyncio.coroutine
def vector_mult(v_a : list, v_b : list) -> list:
    if len(v_a) > split_threshold:
        name = tasksupport.CurrentTask.name
        split = len(v_a)//2

        data = vector_serializer(split_threshold, v_a[:split], v_b[:split])
        task1 = tasksupport.distributed_process(name, data)

        data = vector_serializer(split_threshold, v_a[split:], v_b[split:])
        task2 = tasksupport.distributed_process(name, data)

        return (yield from task1) + (yield from task2)

    return [v_a[i]*v_b[i] for i in range(len(v_a))]

@asyncio.coroutine
def task():
    tasksupport.CurrentTask.finalize((yield from vector_mult(vector_a, vector_b)))
    print("finalized task: ", tasksupport.CurrentTask.task_id)

loop = asyncio.get_event_loop()

try:
    loop.run_until_complete(task())

except:
    print("!error task: ",tasksupport.CurrentTask.task_id)

finally:
    loop.close()