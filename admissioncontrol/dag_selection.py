"""
1. Start with collapsed array of the linearized DAG
2. Iterate through array, at each step trying each configuration, updating the set of resources as you go
3. Recursively call the function at each valid config,
"""
from enum import Enum


class Resource(Enum):
	CPU = [1, 100]
	GPU = [3, 20]


class IOLatency(Enum):
	CPU = 3
	GPU = 9
	Network = 1


class Node:

	def __init__(self, id, resource_type):
		self.id = id
		self.resource_type = resource_type
		self.memory = resource_type.value[1]


class Cluster:

	def __init__(self, nodes):
		self.nodes = nodes


class Function:

	def __init__(self, id, runtimes, max_memory, prev_funcs=set(), next_funcs=set(), dag=None):
		self.id = id
		self.runtimes = runtimes
		self.max_memory = max_memory
		self.prev_funcs = prev_funcs
		self.next_funcs = next_funcs
		self.dag = dag

	def get_resource_runtime(self, resource):
		return self.runtimes[resource]


class DAG:

	def __init__(self, id, root, max_time, max_cost):
		self.id = id
		self.root = root
		self.max_time = max_time
		self.max_cost = max_cost


class FunctionInstance:

	def __init__(self, function, node=None, dag_instance=None, time=0, num_prev_instances=0, max_prev_time=0):
		self.function = function
		self.node = node
		self.dag_instance = dag_instance
		self.time = time
		self.num_prev_instances = num_prev_instances
		self.max_prev_time = max_prev_time


class DAGInstance:

	def __init__(self, dag):
		self.dag = dag
		self.running_time = 0
		self.running_cost = 0
		self.incomplete_map = {}
		self.ready_queue = []
		self.completed_map = {}

	def gen_next_func_instance(self, this_instance):
		for function in this_instance.function.next_funcs:
			next_instance = None
			if function.id in self.incomplete_map:
				next_instance = self.incomplete_map[function.id]
			else:
				next_instance = FunctionInstance(function)

			next_instance.num_prev_instances += 1
			if this_instance.time > next_instance.max_prev_time:
				next_instance.max_prev_time = this_instance.time

			if next_instance.num_prev_instances == len(function.prev_funcs):
				self.ready_queue.append(next_instance)
			else:
				self.incomplete_map[function.id] = next_instance

		self.completed_map[this_instance] = this_instance.node
		return None

	def copy_dag_instance(self, new_time, func_time, resource):
		temp_dag_instance = DAGInstance(self.dag)
		temp_incomplete_map = {}
		temp_ready_queue = []
		temp_completed_map = []

		for key, value in self.incomplete_map.items():
			temp_incomplete_map[key] = FunctionInstance(value.function, value.node, temp_dag_instance, value.time, value.num_prev_instances, value.max_prev_time)

		for ready_func in self.ready_queue:
			temp_ready_queue.append(FunctionInstance(ready_func.function, ready_func.node, temp_dag_instance, ready_func.time, ready_func.num_prev_instances, ready_func.max_prev_time))

		for key_two, value_two in self.incomplete_map.items():
			new_key = FunctionInstance(key_two.function, key_two.node, temp_dag_instance, key_two.time, key_two.num_prev_instances, key_two.max_prev_time)
			temp_incomplete_map[new_key] = value_two

		temp_dag_instance.incomplete_map = temp_incomplete_map
		temp_dag_instance.ready_queue = temp_ready_queue
		temp_dag_instance.completed_map = temp_completed_map

		if new_time > self.running_time:
			temp_dag_instance.running_time = new_time
		else:
			temp_dag_instance.running_time = self.running_time

		temp_dag_instance.running_cost = self.running_cost + (func_time * resource.value[0])

		return temp_dag_instance


def generate_valid_dags(cluster, dag):
	dag_valid_list = []
	dag_ready_queue = []

	root_dag_instance = DAGInstance(dag)
	root_func_instance = FunctionInstance(dag.root)
	root_dag_instance.ready_queue.append(root_func_instance)
	dag_ready_queue.append(root_dag_instance)

	while len(dag_ready_queue) > 0:
		this_dag_instance = dag_ready_queue.pop(0)
		this_func_instance = this_dag_instance.ready_queue.pop(0)
		for resource in Resource:
			temp_func_instance = FunctionInstance(this_func_instance.function, node=resource, num_prev_instances=this_func_instance.num_prev_instances)
			temp_func_time = temp_func_instance.function.get_resource_runtime(resource)
			temp_func_instance.time = this_func_instance.max_prev_time + temp_func_time
			new_dag_instance = this_dag_instance.copy_dag_instance(temp_func_instance.time, temp_func_time, resource)
			new_dag_instance.gen_next_func_instance(temp_func_instance)
			if len(new_dag_instance.ready_queue) == 0:
				dag_valid_list.append(new_dag_instance)
			else:
				dag_ready_queue.append(new_dag_instance)
