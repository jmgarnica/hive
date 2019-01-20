
from drone import Drone
from overlord import Overlord
from pathlib import Path

d = Drone(4004, propagation_port= 4007)

d.loadtask(str(Path("./TestTask/vector_mult.py")))
d.start_work()

with Path("./TestTask/vector_mult_test_data.txt").open() as f:
    data = f.read()

d.addtask("vector_mult", data, [])
