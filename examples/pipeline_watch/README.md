# Trace Haystack Pipelines in Browser <!-- omit in toc -->

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Start application](#start-application)
- [Run pipeline](#run-pipeline)
- [Run a different pipeline](#run-a-different-pipeline)
- [Technical details](#technical-details)
  - [Fronted App](#fronted-app)
  - [Backend App](#backend-app)
- [Known issues](#known-issues)

## Overview

This is a sample application which demonstrates running Haystack pipelines with [Ray Serve](https://docs.ray.io/en/latest/serve/http-guide.html#fastapi-http-deployments).
It consist of backend API to manage pipeline execution as well as fronted app which integrates with the API to trigger pipeline runs and display pipeline events while pipeline is running.

![pipeline-watch-overview](docs/pipeline-watch-overview.png)

1. When frontend app loads it fetches mermaid diagram representation of the pipeline and renders it as SVG on the page. It allows user to visually track pipeline execution and help better understand pipeline steps and order of execution.
2. User can adjust pipeline execution settings. By default we can control component delay in seconds in order to slowdown execution so it is easier to track running pipeline visually. After clicking "Run Pipeline" button pipeline starts running in backend and events are ready for consumption.
3. Pipeline events are streamed to browser using [SSE](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events) and each events is presented as an entry in the "Pipeline Events" area. Event data can be viewed as JSON so user can explore component inputs/outputs in browser.

## Project Structure

```shell
.
├── config.yaml # Configuration file for the app (e.g. set your OpenAI API Key here)
├── pipelines
│   ├── pipeline_basic_rag.py
│   ├── pipeline_react.py # Default Haystack pipeline which runs in backend
│   └── pipeline_web_search.py
├── ray_pipeline_api.py # Backend API (Ray Serve + FastAPI)
├── requirements.txt # Project dependencies
└── ui # Frontend application
    ├── index.html
    ├── index.js
    └── styles.css
```

## Installation

Please take a look at `requirements.txt` to see the list of packages being installed. We make sure both `ray[serve]` and `ray-haystack` are installed. Additional packages are needed to help with SSE event handling and extra pipeline examples (see `pipelines` folder).

```shell
# create a new virtual environment
python -m venv .venv

# activate a virtual environment
source .venv/bin/activate

# optional
pip install --upgrade pip

# install dependencies
pip install -r requirements.txt
```

> **Note**
> The steps above were tested with python version `3.8.18`.

## Start application

The recommended way to start the app is to run `serve run config.yaml` command. `config.yaml` was generated in order to simplify application startup, but most importantly it allows to configure [runtime environment](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments).

Before you start the app make sure you provide environment variables for the sample Haystack pipeline which is by default runs by backend.
Open `config.yaml` and provide both `OPENAI_API_KEY` and `SERPERDEV_API_KEY`.

The default pipeline used in the app was borrowed from [ReAct Haystack Notebook](https://github.com/vblagoje/notebooks/blob/main/haystack2x-demos/ReAct_Haystack.ipynb). Once you provided API keys start the app in your terminal:

```shell
serve run config.yaml
```

Wait for the app to be available at [](http://localhost:8000/) (should take at least 10-20 seconds depending on your hardware).

## Run pipeline

You should be able to see the "Run Pipeline" button. Push the button and with proper API Keys in `config.yaml` you should see pipeline events flowing.

https://github.com/user-attachments/assets/e6d077c6-1630-46d0-986b-b3a0dd1d6bba

> **Important**
> Because of existing UI bugs the app might render with issues initially. Please pay attention to SVG controls, if not displayed properly (e.g. during zooming in/out) - please refresh the page. Pipeline inputs (settings) could also render with issues - refresh the page as well in such case. It is going to be fixed in future iterations.

Pipeline events are streamed until pipeline finishes running. You can initiate run once again afterwards. Additional controls are available to reset colors in the diagram sa well as clean pipeline events area.

By tweaking "delay" property in "Pipeline Settings" it is possible to slowdown execution as per your requirement.
In addition to that with "Pipeline Inputs" you can adjust input values for certain components before starting a pipeline.

## Run a different pipeline

The [ReAct Haystack Pipeline](pipelines/pipeline_react.py) is the default pipeline used in `pipeline_watch`. In the `pipelines` directory you can find two more examples:

- [Basic RAG](pipelines/pipeline_basic_rag.py) which is based on [Creating Your First QA Pipeline with Retrieval-Augmentation](https://haystack.deepset.ai/tutorials/27_first_rag_pipeline) tutorial. It demonstrates the usage of `SentenceTransformersTextEmbedder` component as well as `RayInMemoryDocumentStore`
- [Websearch](pipelines/pipeline_web_search.py) is based on the [Building Fallbacks to Websearch with Conditional Routing](https://haystack.deepset.ai/tutorials/36_building_fallbacks_with_conditional_routing) tutorial

In order to use another pipeline you should import it in [ray_pipeline_api.py](ray_pipeline_api.py):

```python
# Change from `pipeline_react`
# from pipelines.pipeline_react import create_pipeline, default_pipeline_inputs

# To `pipeline_web_search`
from pipelines.pipeline_web_search import create_pipeline, default_pipeline_inputs
```

In case you would like to use your very own pipeline make sure you follow same "interface" as in existing examples (e.g. expose `create_pipeline` & `default_pipeline_inputs` factory functions):

- Make sure you also provide necessary environment variables in `config.yaml` file.
- It would be a good practice to first run your pipeline as usual (e.g. `python pipeline_web_search.py`) as it would be much easier to analyze errors if any. `pipeline_watch` needs improvements with better error handling and reporting.
- If you pipeline uses additional dependencies do not forget to update `requirements.txt` and install those before starting the app

## Technical details

### Fronted App

The goal was to create UI with less effort, focus on the objective - which is consume pipeline events and display event's data for user to explore:

- [htmx](https://htmx.org/) with [sse](https://htmx.org/extensions/sse/) and [client-side-templates](https://github.com/bigskysoftware/htmx-extensions/blob/main/src/client-side-templates/README.md) extensions seems like a good declarative approach toconsume events from backend API (Ray Serve)
- Most of dependencies (JS) are included as external scripts in the `head` section of the page. In particular [shoelace.style](https://shoelace.style/) components were quite helpful. [json-viewer](https://www.npmjs.com/package/@andypf/json-viewer) component has also greatly reduced efforts to build event's data viewer.

### Backend App

Running a [FastAPI with Ray Serve](https://docs.ray.io/en/latest/serve/http-guide.html) seemed like a natural choice as internally pipeline can start running within already existing ray environment (e.g. no need to submit ray jobs). With [`config.yaml`](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html#specify-parameters-through-the-serve-config-file) configuration it was easy to manage environment variables.

An interesting improvement would be to try and run in in kubernetes with KubeRay.

## Known issues

1. UI application might start with some issues, e.g. mermaid diagram could vanish when you zoom in/out, or pipeline inputs editor (json editor) could appear broken. Just refresh page in this case and that should help. Those issues will be addressed later on.
2. In some rare cases, depending on which component you are using, you could get error related to how component's data is serialized to json before is being sent to UI. In that case the problem could be solved by modifying slightly the `EnhancedJSONEncoder` class in `ray_pipeline_api.py`. It covers already most of cases (at least all the examples were tested without any issues).
3. If there is error during pipeline execution it might be not obvious where exactly error happened, which is annoying but can be improved. Partly that is because of lack of good error handling in event streaming endpoint. Thats why it is better to first test your custom pipeline by running from terminal and then as part of the `pipeline_watch` app.
4. If you start pipeline from UI and then refresh the page, pipeline will still run in backend but pipeline events will no longer be reported in UI.
