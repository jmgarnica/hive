from threading import local

TaskData = local()

async def distributed_process(task : str, data : str,  dependencies : list = None):
    if not "drone" in TaskData.__dict__:
        raise Exception("no drone attached")

    task = TaskData.drone.addtask(task, data, dependencies, TaskData.task.task_id)
    
    if task == None:
        raise Exception("could not start the task")

    return TaskData.drone.await_task(task)
