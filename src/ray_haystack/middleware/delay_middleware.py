import time
from typing import Literal, Optional

from ray_haystack.middleware.middleware import ComponentMiddleware


class DelayMiddleware(ComponentMiddleware):
    def __init__(
        self,
        delay: float = 1,
        delay_type: Optional[Literal["before", "after", "before_and_after"]] = None,
    ):
        self.delay = delay
        self.delay_type = delay_type or "before"

    def __call__(self, component_input, context):
        if self.delay_type in ("before", "before_and_after"):
            time.sleep(self.delay)

        outputs = self.next(component_input, context)

        if self.delay_type in ("after", "before_and_after"):
            time.sleep(self.delay)

        return outputs
