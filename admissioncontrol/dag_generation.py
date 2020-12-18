"""
1. Start with collapsed array of the linearized DAG
2. Iterate through array, at each step trying each configuration, updating the set of resources as you go
3. Recursively call the function at each valid config,
"""
from enum import Enum


class Resource(Enum):
	CPU = [1, 100]
	GPU = [3, 20]


class Function:

	def __init__(self, id, runtimes, max_memories, prev_funcs=set(), next_funcs=set(), dag=None):
		self.id = id
		self.runtimes = runtimes
		self.max_memories = max_memories
		self.prev_funcs = prev_funcs
		self.next_funcs = next_funcs
		self.dag = dag

	def get_max_memory(self, resource):
		return self.max_memories[resource]

	def get_resource_runtime(self, resource):
		return self.runtimes[resource]


class DAG:

	def __init__(self, id, root, num_funcs):
		self.id = id
		self.root = root
		self.num_funcs = num_funcs

	def gen_dep_queue(self):
		dependency_queue = []
		gen_queue = []
		incomplete_map = {}
		gen_queue.append(self.root)
		while len(dependency_queue) < self.num_funcs:
			temp_func = gen_queue.pop(0)
			for next_function in temp_func.next_funcs:
				next_eval = 1
				if next_function.id in incomplete_map:
					next_eval += incomplete_map[next_function.id]
				if next_eval == len(next_function.prev_funcs):
					gen_queue.append(next_function)
					incomplete_map.pop(next_function.id, None)
				else:
					incomplete_map[next_function.id] = next_eval
			dependency_queue.append(temp_func)
		return dependency_queue


class DAGInstance:

	def __init__(self, dag):
		self.dag = dag
		self.running_time = 0
		self.running_cost = 0
		self.functions_per_resource = {}
		self.id_res_map = {}
		self.id_max_map = {}

		for res in list(Resource):
			self.functions_per_resource[res] = []

	def add_func_res(self, function, resource):
		func_tuple = (function.id, function.get_max_memory(resource))
		self.functions_per_resource[resource].append(func_tuple)

	def copy_dag_instance(self):
		new_dag_instance = DAGInstance(self.dag)
		for id_one, res in self.id_res_map:
			new_dag_instance.id_res_map[id_one] = res
		for id_two, max_prev in self.id_max_map:
			new_dag_instance.id_max_map[id_two] = max_prev
		for i in range(len(self.functions_per_resource)):
			for func_tuple in self.functions_per_resource[i]:
				new_tuple = (func_tuple[0], func_tuple[1])
				new_dag_instance.functions_per_resource[i].append(new_tuple)
		new_dag_instance.running_cost = self.running_cost
		new_dag_instance.running_time = self.running_time
		return new_dag_instance

	def update_dag_instance(self, func, res):
		self.id_res_map[func.id] = res
		func_time = func.get_resource_runtime(res) + self.id_max_map[func.id]
		for root_next_func in func.next_funcs:
			next_max_time = 0
			if root_next_func.id in self.id_max_map:
				next_max_time = self.id_max_map[root_next_func.id]
			self.id_max_map[root_next_func.id] = max(func_time, next_max_time)
		self.running_time = max(self.running_time, func_time)
		self.running_cost = self.running_cost + res.value[0]
		self.add_func_res(func, res)
		self.id_max_map.pop(func.id, None)


def gen_dag_instances(dag):
	dep_queue = dag.gen_dep_queue()
	instance_list = []

	root = dep_queue.pop(0)
	for root_res in list(Resource):
		root_dag_instance = DAGInstance(dag)
		root_dag_instance.id_res_map[root.id] = root_res
		for root_next_func in root.next_funcs:
			root_dag_instance.id_max_map[root_next_func.id] = root.get_resource_runtime(root_res)
		root_dag_instance.running_time = root.get_resource_runtime(root_res)
		root_dag_instance.running_cost = root_res.value[0]
		root_dag_instance.add_func_res(root, root_res)
		instance_list.append(root_dag_instance)

	while len(dep_queue) > 0:
		function = dep_queue.pop(0)
		new_instance_list = []
		for dag_instance in instance_list:
			for res in list(Resource):
				new_dag_instance = dag_instance.copy_dag_instance()
				new_dag_instance.update_dag_instance(function, res)
				new_instance_list.append(new_dag_instance)
		instance_list = new_instance_list

	for finished_dag_instance in instance_list:
		for func_res in list(finished_dag_instance.functions_per_resource.values()):
			sorted(func_res, key=lambda x: x[1])

	return instance_list


def select_pareto_instances(instance_list):
	pareto_list = []

	for instance in instance_list:
		pareto_add = True
		for comp_instance in instance_list:
			if not (instance is comp_instance):
				if (comp_instance.running_time <= instance.running_time) and (comp_instance.running_cost <= instance.running_cost):
					pareto_add = False
					break
		if pareto_add:
			pareto_list.append(instance)

	return pareto_list
