import asyncio
import base64
import json
import logging
import time
from dataclasses import asdict, is_dataclass
from typing import Any, AsyncGenerator, Dict

from fastapi import FastAPI, Form
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from mergedeep import merge
from pipelines.pipeline_react import create_pipeline, default_pipeline_inputs
from pydantic import BaseModel, Field, Json
from ray import serve
from sse_starlette import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware
from typing_extensions import Annotated

logger = logging.getLogger("ray.serve")


app = FastAPI(title="pipeline-watch")
api = FastAPI(title="pipeline-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, json_value):
        if isinstance(json_value, bytes):
            return base64.b64encode(json_value).decode("ascii")
        if is_dataclass(json_value):
            return asdict(json_value)
        if isinstance(json_value, BaseModel):
            return json_value.model_dump()
        if isinstance(json_value, set):
            return list(json_value)

        return super().default(json_value)


class PipelineSubmitData(BaseModel):
    pipeline_inputs: Json[Any] = Field(..., alias="pipeline-inputs", default_factory=dict)
    pipeline_settings: Json[Any] = Field(..., alias="pipeline-settings", default_factory=dict)


@serve.deployment
@serve.ingress(app)
class RayPipelineApi:
    def __init__(self):
        self.pipeline = create_pipeline()

    @app.get("/")
    def index(self):
        return FileResponse("ui/index.html")

    @app.post("/pipeline/submit")
    def submit(self, data: Annotated[PipelineSubmitData, Form()]):
        pipeline_inputs = merge(
            default_pipeline_inputs(),
            data.pipeline_inputs,
        )

        self.results = self.pipeline.run_nowait(pipeline_inputs, ray_settings=data.pipeline_settings)

        pipeline_id = "pipeline_id"  # TODO: Decide whether pipeline runs should be stored internally by ids
        return {"pipeline_id": pipeline_id, "eventsUrl": f"/pipeline/events?pipeline_id={pipeline_id}"}

    @app.get("/pipeline/events")
    def events(self, pipeline_id: str):
        return EventSourceResponse(self.generate_pipeline_events(pipeline_id))

    @app.get("/pipeline/mermaid")
    def mermaid(self):
        return PlainTextResponse(content=self.pipeline.to_mermaid_text())

    async def generate_pipeline_events(self, _pipeline_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        start = time.time()
        queue = self.results.events_queue

        try:
            yield {"event": "start", "data": json.dumps({"timestamp": start})}

            while pipeline_event := await queue.get_async():
                yield {"event": "data", "data": self._convert_to_json(pipeline_event)}

                if pipeline_event.type == "ray.haystack.pipeline-end":
                    break

            yield {"event": "end", "data": json.dumps({"time": time.time() - start})}
        except asyncio.CancelledError:
            yield {"event": "end", "data": json.dumps({"time": time.time() - start})}

    def _convert_to_json(self, data):
        try:
            return json.dumps(data, cls=EnhancedJSONEncoder)
        except Exception as err:
            logger.error(f"Error converting pipeline event data to JSON: {err}")
            return f"{'error': '{str(err)}'}"


ray = RayPipelineApi.bind()  # type:ignore
