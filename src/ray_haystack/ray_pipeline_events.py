import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


def _generate_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class PipelineEventType(str, Enum):
    PIPELINE_START = "ray.haystack.pipeline-start"
    PIPELINE_END = "ray.haystack.pipeline-end"
    COMPONENT_START = "ray.haystack.component-start"
    COMPONENT_END = "ray.haystack.component-end"


@dataclass
class PipelineEvent:
    source: str
    type: str
    time: str = field(default_factory=_generate_timestamp)
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStartEvent(PipelineEvent):
    source: str = "ray-pipeline-processor"
    type: str = PipelineEventType.PIPELINE_START


@dataclass
class PipelineEndEvent(PipelineEvent):
    source: str = "ray-pipeline-processor"
    type: str = PipelineEventType.PIPELINE_END


@dataclass
class ComponentStartEvent(PipelineEvent):
    source: str = "ray-pipeline-processor"
    type: str = PipelineEventType.COMPONENT_START


@dataclass
class ComponentEndEvent(PipelineEvent):
    source: str = "ray-pipeline-processor"
    type: str = PipelineEventType.COMPONENT_END
