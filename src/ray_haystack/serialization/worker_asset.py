import functools
import importlib
from collections import defaultdict
from typing import Callable, ClassVar, Dict, List, Optional, Set, Type, Union

WorkerAssetRegistry = Dict[str, Set["WorkerAsset"]]


class WorkerAsset:
    registry: ClassVar[WorkerAssetRegistry] = defaultdict(set)

    @classmethod
    def get_assets(cls, component_name: Optional[str] = None) -> List["WorkerAsset"]:
        common_assets = cls.registry["WorkerAssetRegistry__common__"]
        for_component = cls.registry[component_name] if component_name else []
        return [*common_assets, *for_component]

    def __init__(self, func: Union[Type, Callable], *, for_components: Optional[List[str]] = None):
        functools.update_wrapper(self, func)

        self._func = func
        self._for_components = for_components or []

        if for_components:
            for component_name in for_components:
                WorkerAsset.registry[component_name].add(self)
        else:
            WorkerAsset.registry["WorkerAssetRegistry__common__"].add(self)

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def import_asset(self):
        mod_name = self._func.__module__
        module = importlib.import_module(mod_name)
        if module is None:
            spec = importlib.machinery.ModuleSpec(mod_name, None)
            module = importlib.util.module_from_spec(spec)

        setattr(module, self._func.__name__, self._func)


def worker_asset(function=None, *, for_components: Optional[List[str]] = None):
    if function:
        return WorkerAsset(function)
    else:

        def wrapper(function):
            return WorkerAsset(function, for_components=for_components)

        return wrapper
