from typing import Any, Dict, List, Optional, Type

from haystack.core.component import Component
from haystack.core.errors import DeserializationError
from haystack.core.serialization import component_from_dict, component_to_dict

from ray_haystack.serialization.worker_asset import WorkerAsset


class ComponentWrapper:
    """
    ComponentWrapper wraps an instance of Haystack component to help with its serialization.
    Haystack components should be serialized before they move across boundaries - processes or remote nodes in a
    distributed environment. For that native Haystack [serialization protocol](https://docs.haystack.deepset.ai/v2.0/docs/serialization#performing-custom-serialization)
    is used where each component provides `to_dict` and `from_dict` methods, so before component is moved across
    boundaries its data is serialized with `to_dict` (see `component_to_dict` implementation).

    The choice of using `to_dict` instead of relying on out of the box [Ray serialization](https://docs.ray.io/en/latest/ray-core/objects/serialization.html)
    with Pickle was made because we do not know (and do not control) how each component manages its internal state - it
    might be not friendly with how Pickle works.

    The `__reduce__` method implements simple serialization steps, specifying how to re-create `ComponentWrapper` class
    with respective constructor arguments.

    There might be cases when during de-serialization `component_from_dict` will raise exception because certain types
    could not be created on a remote process (or machine). With `WorkerAsset` it should be possible to specify which
    python types should be imported before de-serialization happens. In local setup one would not care much about that,
    but in distributed environment before component is re-created we should provide all dependencies (e.g.
    module imports, python functions)
    """

    def __init__(
        self,
        component_class: Optional[Type] = None,  # Should be only used during deserialization
        component_data: Optional[Dict[str, Any]] = None,  # Should be only used during deserialization
        assets: Optional[List[WorkerAsset]] = None,  # Should be only used during deserialization
        name: Optional[str] = None,
        component: Optional[Component] = None,
    ):
        self._assets = assets or []
        self._name = name

        if component:
            self._component = component
            self._assets = self._collect_assets()
        elif component_class and component_data:
            self._import_assets()
            self._component = component_from_dict(component_class, component_data, None)
        else:
            raise DeserializationError(
                "You should either provide component instance to the wrapper or the \
                                       combination of component_class and component_data (serialized component)"
            )

    def get_component(self):
        return self._component

    def _collect_assets(self):
        return WorkerAsset.get_assets(self._name)

    def _import_assets(self):
        if self._assets:
            for asset in self._assets:
                asset.import_asset()

    def __reduce__(self):
        # we return a tuple of class_name to call, and parameters to pass when re-creating
        return (
            self.__class__,
            (self._component.__class__, component_to_dict(self._component), self._assets, self._name),
        )
