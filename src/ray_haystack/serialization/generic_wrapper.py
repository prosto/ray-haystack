from typing import Any, Dict, Generic, Optional, Type, TypeVar

from haystack.core.errors import DeserializationError, SerializationError

R = TypeVar("R")


class GenericWrapper(Generic[R]):
    def __init__(
        self,
        cls: Optional[Type] = None,
        data: Optional[Dict[str, Any]] = None,
        obj: Optional[R] = None,
    ):
        if obj:
            self._obj = obj
        elif cls and data and hasattr(cls, "from_dict"):
            self._obj = cls.from_dict(data)
        else:
            raise DeserializationError(
                "You should either provide object instance to the wrapper or the "
                "combination of type and data (serialized object)"
            )

    def get_obj(self) -> R:
        return self._obj

    def __reduce__(self):
        if not hasattr(self._obj, "to_dict"):
            raise SerializationError("Objects which do not implement `to_dict` are considered non-serializable")

        return (
            self.__class__,
            (
                self._obj.__class__,
                self._obj.to_dict(),
            ),
        )
