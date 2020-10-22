'''
Known Issues: 
 - If subsequent functions are going from CPU->CPU or GPU->GPU we still include transfer time
 - We don't time individual functions
 - We can't run multiple functions on a GPU together
 - GPU cold/warm starts
'''


from collections import namedtuple, deque
from enum import Enum
from typing import List

RUNNING_TIME = 100
NUM_CPUS = 1
NUM_GPUS = 1


class ResourceType(Enum):
	CPU = 1
	GPU = 2

Function = namedtuple('Function', 
	[
		'unique_id',              # We need this to refer to individual functions in our DAG
		'request_time',           # Time the request arrives into our system
		'cpu_time',               # Time in CPU
		'transfer_into_cpu_time', # Transfer from previous function's GPU into our CPU (else 0)
		'gpu_time',               # Time in GPU
		'transfer_into_gpu_time', # Transfer from previous function's CPU into our GPU (else 0)
		'next_functions',         # List of subsequent unique_id's if more functions in DAG 
	]
)
ResourceQueue = namedtuple('ResourceQueue', ['queue', 'transferring', 'wait_time'])
Resources = namedtuple('Resources', 
	[
		'cpus',     # List: Being run + waiting to transfer on CPUs (index refers to which CPU)
		'gpus',     # List: Being run + waiting to transfer on GPUs (index refers to which GPU)
	]
)

id_function_map = {
	'linear_first': Function(
		unique_id='linear_first', 
		request_time=0, 
		cpu_time=3, 
		transfer_into_cpu_time=0, 
		gpu_time=3, 
		transfer_into_gpu_time=1,
		next_functions=['linear_second'],
	),
	'linear_second': Function(    # This function takes a long time to run on a CPU
		unique_id='linear_second', 
		request_time=-1,          # Doesn't matter since it's always after linear_first
		cpu_time=5, 
		transfer_into_cpu_time=1, 
		gpu_time=1, 
		transfer_into_gpu_time=1,
		next_functions=['linear_third'],
	),
	'linear_third': Function(     # This function takes a long time to run on a GPU
		unique_id='linear_third', 
		request_time=-1,          # Doesn't matter since it's always after linear_second
		cpu_time=1, 
		transfer_into_cpu_time=1, 
		gpu_time=5, 
		transfer_into_gpu_time=1,
		next_functions=[],
	)
}


def get_start_functions_resources():
	linear_dag = 'linear_first' # Starting function; TODO: We want to keep track of latency for this DAG
	branch_dag = 'branch_first'

	all_dags       = [linear_dag] # branch_dag, etc. will also go here
	next_functions = [id_function_map[dag] for dag in all_dags]
	next_functions.sort(key=lambda function: function.request_time)

	resources = Resources( # 1 CPU, 1 GPU
		cpus=[ResourceQueue(queue=deque(), transferring=list(), wait_time=0) for _ in range(NUM_CPUS)],        
		gpus=[ResourceQueue(queue=deque(), transferring=list(), wait_time=0) for _ in range(NUM_GPUS)], 
	)
	return next_functions, resources


def process_resource_list_queues(resources: List[ResourceQueue], next_functions: List[Function], time: int, resource_type):
	for resource in resources:
		remove_from_transferring = set()
		for function, expiry_time in resource.transferring:
			if time >= expiry_time:
				queue_time = function.cpu_time if resource_type == ResourceType.CPU else function.gpu_time
				resource.queue.append((function, time + queue_time))
				remove_from_transferring.add((function.unique_id, expiry_time))
		for to_remove_id, to_remove_time in remove_from_transferring:
			resource.transferring.remove((id_function_map[to_remove_id], to_remove_time))

		removed = True
		while resource.queue and removed:
			function, expiry_time = resource.queue.popleft()
			if time >= expiry_time:
				for next_function in function.next_functions:
					next_functions.insert(0, id_function_map[next_function])
			else:
				resource.queue.appendleft((function, expiry_time))
				removed = False


def process_cpu_gpu_list_queues(resources: Resources, next_functions: List[Function], time: int): 
	# For each time step, updates list and queues on each CPU/GPU
	# Our goal: can we optimize this function!?
	process_resource_list_queues(resources.cpus, next_functions, time, ResourceType.CPU)
	process_resource_list_queues(resources.gpus, next_functions, time, ResourceType.GPU)


def schedule_function(next_function: Function, resources: Resources, time: int): 
	# Our goal: can we optimize this function!?
	min_cpu = min(resources.cpus, key=lambda cpu: cpu.wait_time) #TODO: Fix, wait_time now is always 0
	min_gpu = min(resources.gpus, key=lambda gpu: gpu.wait_time)

	cpu_time = next_function.transfer_into_cpu_time + min_cpu.wait_time
	gpu_time = next_function.transfer_into_gpu_time + min_gpu.wait_time
	if cpu_time < gpu_time: # Better to schedule on the CPU
		#TODO: We currently don't keep track of if our function is on CPU/GPU for transfer (i.e CPU->CPU should be 0)
		min_cpu.transferring.append((next_function, time + next_function.transfer_into_cpu_time)) 
	else:                   # Better to schedule on the GPU
		min_cpu.transferring.append((next_function, time + next_function.transfer_into_gpu_time))


if __name__ == "__main__":
	next_functions, resources = get_start_functions_resources()
	overall_latency = 0
	for time in range(RUNNING_TIME):
		while next_functions and time >= next_functions[0].request_time:
			print(time, next_functions)
			process_cpu_gpu_list_queues(resources, next_functions, time)
			next_function, overall_latency = next_functions.pop(0), time
			schedule_function(next_function, resources, time)
		process_cpu_gpu_list_queues(resources, next_functions, time)

	print("Time all functions finished is:", overall_latency)