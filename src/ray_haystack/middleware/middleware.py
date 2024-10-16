from typing import Any, Callable, Dict, TypedDict

from haystack.core.errors import DeserializationError
from haystack.core.serialization import default_from_dict, import_class_by_name


class ComponentMiddlewareContext(TypedDict, total=False):
    component_name: str


ComponentMiddlewareFunc = Callable[[Dict[str, Any], ComponentMiddlewareContext], Any]


class ComponentMiddleware:
    def set_next(self, next: ComponentMiddlewareFunc):
        self.next = next

    def __call__(self, _component_input: Dict[str, Any], _context: ComponentMiddlewareContext) -> Any:
        pass


def middleware_from_dict(middleware_data: Dict[str, Any]) -> ComponentMiddleware:
    try:
        middleware_class = import_class_by_name(middleware_data["type"])
    except ImportError as e:
        raise DeserializationError(f"Class '{middleware_data['type']}' not correctly imported") from e
    if hasattr(middleware_class, "from_dict"):
        return middleware_class.from_dict(middleware_data)
    else:
        return default_from_dict(middleware_class, middleware_data)
