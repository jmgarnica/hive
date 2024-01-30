# Hive (University project)
Distributed system for parallelization of tasks

## Description
The objective of this system is to execute a large volume of work using the parallelization capabilities of a computer network and interact with customers as a single entity. 
As additional features are the automatic extensibility of the network and a high fault tolerance based on the replication of tasks. The system will execute any type of task, provided that it meets the requirements defined in Use and definition of tasks.
Hive does not provide protection against attacks or malicious use of any component since its design supposes a friendly environment.

The design of components and functionalities is based on a superpeer architecture, this can be understood as a client-server architecture, where the server is a distributed peer-to-peer system.
The member nodes of this subsystem manage the distribution of the tasks that the client nodes must execute, to organize the replication of tasks and the load balance, they are linked in a ring, where each node knows its next and previous one.

For more detail see the (Spanish) [documentation](doc/Hive.pdf)
