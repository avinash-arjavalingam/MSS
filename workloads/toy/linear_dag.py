from simulator.dag import Dag, Function
from simulator.resource import ResourceType
from simulator.runtime import ConstantTime

linear_first = Function(
	unique_id='linear_first',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 0.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(3),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 0.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(1),
			'post' : ConstantTime(0)
		}
	}
)

linear_second = Function(    # This function takes a long time to run on a CPU
	unique_id='linear_second',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 0.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(5),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 0.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(1),
			'post' : ConstantTime(0)
		}
	}
)

linear_third = Function(     # This function takes a long time to run on a GPU
	unique_id='linear_third',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 0.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(1),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 0.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(5),
			'post' : ConstantTime(0)
		}
	}
)

linear_dag = Dag('linear', funs=[linear_first, linear_second, linear_third])
linear_dag.add_edge(linear_first, linear_second)
linear_dag.add_edge(linear_second, linear_third)
linear_dag.sanity_check()