import logging
from typing import Any, Dict, Optional, cast

import ray

from ray_haystack.middleware.middleware import (
    ComponentMiddlewareContext,
    ComponentMiddlewareFunc,
    middleware_from_dict,
)
from ray_haystack.ray_pipeline_settings import ComponentMiddleware
from ray_haystack.serialization import ComponentWrapper


def setup_logger(name: str):
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


@ray.remote
class ComponentActor:
    def __init__(
        self, name: str, component: ComponentWrapper, middleware: Optional[Dict[str, ComponentMiddleware]] = None
    ):
        self.name = name
        self.component_wrapper = component

        self.inputs: Dict[str, Any] = {}
        self.outputs: Dict[str, Any] = {}

        self._warmed_up: Optional[bool] = None
        self.logger = setup_logger(f"haystack.ray.component-{name}")

        # Create middleware chain from component settings
        self._middleware_chain = self._build_middleware_chain(middleware or {})

    def run_component(self, inputs: Dict[str, Any]):
        context: ComponentMiddlewareContext = {"component_name": self.name}
        return self._middleware_chain(inputs, context)

    def _run_component(self, inputs: Dict[str, Any], _context):
        self.inputs.update(inputs)

        self._warm_up_if_needed()

        result = self._get_component().run(**self.inputs)

        if isinstance(result, dict):
            self.outputs.update(result)

        return (self.name, result)

    def _build_middleware_chain(self, middleware: Dict[str, ComponentMiddleware]) -> ComponentMiddlewareFunc:
        next_handler: ComponentMiddlewareFunc = self._run_component

        for name, params in middleware.items():
            self.logger.debug(f"Add middleware '{name}' for '{self.name}' to chain.")
            handler = middleware_from_dict(cast(Dict[str, Any], params))
            handler.set_next(next_handler)
            next_handler = handler

        return next_handler

    def _warm_up_if_needed(self):
        component = self._get_component()
        if hasattr(component, "warm_up") and not self._warmed_up:
            self.logger.info(f"Warming up component {self.name}...")
            component.warm_up()
            self._warmed_up = True

    def _get_component(self):
        return self.component_wrapper.get_component()

    def __repr__(self):
        return f"ComponentActor([{self.name}])"
