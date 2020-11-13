from enum import Enum
from typing import Dict, Tuple, List
import sys

from dag import Function
from runtime import Runtime


class ResourceType(Enum):
    CPU = 1
    GPU = 1


class Resource:
    name: str
    typ: ResourceType
    active: Dict[Tuple[str, str], Tuple[float, int]]
    available_space: float
    nearest_finish: int

    def __init__(self, _name: str, _typ: ResourceType):
        self.name = _name
        self.typ = _typ
        self.active = {}
        self.available_space = 100.0
        self.nearest_finish = sys.maxsize

    def add_function(self, fun: Function, tag: str, curr_time: int, *args, **kwargs) -> bool:
        needed_space = fun.resources[self.name]['space']
        if self.can_add_function(fun, tag):
            self.available_space -= needed_space
            rt: Runtime = fun.resources[self.name]['execute']
            finish_time = curr_time + rt.get_runtime(*args, **kwargs)
            self.active[(fun.unique_id, tag)] = (needed_space, finish_time)
            if finish_time < self.nearest_finish:
                self.nearest_finish = finish_time
            return True
        else:
            return False

    def can_add_function(self, fun: Function, tag: str) -> bool:
        assert (fun.unique_id, tag) not in self.active.keys(), \
            f"Function: {fun.unique_id} and tag: {tag} is already allocated on this resource."
        assert self.name in fun.resources, f"Function: {fun.unique_id} cannot be scheduled on this resource"
        assert 'space' in fun.resources[self.name], \
            "Function needs to define the amount of space it needs on this resource."
        assert 'execute' in fun.resources[self.name], \
            "Function needs to define the amount of time it takes to execute on this resource."
        needed_space = fun.resources[self.name]['space']
        if self.available_space >= needed_space:
            return True
        else:
            return False

    def remove_function(self, fun: Function, tag: str, curr_time: int):
        fname = fun.unique_id
        self.__remove_helper(fname, tag, curr_time)

    def remove_at_time(self, curr_time: int) -> List[Tuple[str, str]]:
        removed_functions = []
        for k, v in self.active.copy().items():
            if curr_time >= v[1]:
                self.__remove_helper(k[0], k[1], curr_time)
                removed_functions.append(k)
        return removed_functions

    def __remove_helper(self, fname: str, tag: str, curr_time: int):
        assert curr_time >= self.active[fname][1], "Function execution must finish before removal."
        assert fname in self.active, f"Function {fname} is not allocated on this resource"
        (freed_space, finish_time) = self.active.pop((fname, tag))
        self.available_space += freed_space
        assert 0.0 <= self.available_space <= 100.0, "Avaailable space must always represent a percentage."
        if finish_time == self.nearest_finish:
            self.nearest_finish = self.__find_nearest_finish()

    def __find_nearest_finish(self) -> int:
        finish_times = []
        for k, v in self.active.items():
            finish_times.append(v[1])
        return min(finish_times)


class ResourcePool:
    pass


