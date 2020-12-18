"""
1. With sorted Pareto list, select highest price, highest latency that works
2. Randomly sample from the set
"""

from enum import Enum
from .dag_generation import *
from math import ceil, floor
from random import randint, sample
from bisect import bisect


class Node:

	def __init__(self, id, resource_type):
		self.id = id
		self.resource_type = resource_type
		self.memory = resource_type.value[1]


class Cluster:

	def __init__(self, nodes):
		self.nodes = nodes
		self.nodes_by_res = {}
		for res in Resource:
			self.nodes_by_res[res] = []
		for node in nodes:
			self.nodes_by_res[node.resource_type].append(node)


class DAGSelector:

	def __init__(self, instance_list, sample_size):
		self.price_list = sorted(instance_list, key=lambda x: x.running_cost)
		self.time_list = sorted(instance_list, key=lambda x: x.running_time)
		self.sample_size = int(max(min(sample_size, len(self.price_list)), 1))

	def binary_find_index(self, value, this_list):
		pos = (bisect(this_list, value, 0, len(this_list))) - 1
		return pos

	def get_sample_list(self, price_slo, time_slo):
		sample_list = []
		price_index = self.binary_find_index(price_slo, self.price_list)
		time_index = self.binary_find_index(time_slo, self.time_list)
		if (price_index < 0) or (time_index < 0):
			return []

		end_index = len(self.price_list) - time_index
		valid_size = price_index - end_index
		if valid_size <= 0:
			return []

		valid_list = self.price_list[end_index, price_index]
		min_size = min(self.sample_size, len(valid_list))
		sample_list = sample(valid_list, min_size)
		return sample_list

	def get_placements(self, cluster, sample_instance):
		function_place_map = {}
		for res, res_list in list(sample_instance.functions_per_resource.items()):
			res_nodes = cluster.nodes_by_res[res]
			updated_nodes = []
			for func_id, func_mem in res_list:
				placed = False
				done = False
				while (not placed) and (not done):
					if len(res_nodes) == 0:
						done = True
					elif func_mem <= res_nodes[0].memory:
						function_place_map[func_id] = res_nodes[0].id
						res_nodes[0].memory = res_nodes[0].memory - func_mem
						placed = True
					else:
						popped_node = res_nodes.pop(0)
						updated_nodes.append(popped_node)
				if done:
					break
			if len(res_nodes) == 0:
				cluster.nodes_by_res = sorted(updated_nodes, key=lambda x: x.memory)
				return {}
			else:
				res_nodes.extend(updated_nodes)
				cluster.nodes_by_res = sorted(res_nodes, key=lambda x: x.memory)

		return function_place_map




# if len(this_list) <= 0:
		# 	return None
		#
		# if len(this_list) == 1:
		# 	if value > this_list[0]:
		# 		return 0
		# 	else:
		# 		return -1
		#
		# max_mid = int(ceil(float(len(this_list) - 1) / 2))
		# min_mid = int(floor(float(len(this_list) - 1) / 2))
		#
		# if (value < this_list[max_mid]) and (value > this_list[min_mid]):
		# 	return min_mid
		#
		# if value == this_list[max_mid]:
		# 	return max_mid
		#
		# if value == this_list[min_mid]:
		# 	return min_mid
		#
		# if value < this_list[min_mid]:
		# 	return self.binary_find_index(value, this_list[:min_mid])
		# else:
		# 	return max_mid + self.binary_find_index(value, this_list[max_mid:])



# valid_indexes = list(range(0, valid_size))
		# sample_set_ind = []
		# for i in range(min_size):
		# 	rand_sample = randint(0, len(valid_indexes) - 1)
		# 	rand_ind = valid_indexes.pop(rand_sample)
		# 	sample_set_ind.append(rand_ind)
		#
		# for selected_ind in sample_set_ind:
		# 	sample_list.append(valid_list[selected_ind])