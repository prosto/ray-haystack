from typing import Optional

from haystack.core.component import component


@component
class TransientValue:
    """
    Pass through provided value.
    """

    @component.output_types(value=int)
    def run(self, value: Optional[int] = 1):
        return {"value": value}
