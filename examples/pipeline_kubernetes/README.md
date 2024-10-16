# Running Haystack Pipeline on Kubernetes <!-- omit in toc -->

- [Overview](#overview)
- [Project structure](#project-structure)
- [Setup RayCluster](#setup-raycluster)
  - [Step 1: Create a Kubernetes cluster](#step-1-create-a-kubernetes-cluster)
  - [Step 2: Deploy a KubeRay operator](#step-2-deploy-a-kuberay-operator)
  - [Step 3: Deploy a RayCluster custom resource](#step-3-deploy-a-raycluster-custom-resource)
  - [Check Ray and Python versions on running cluster pods](#check-ray-and-python-versions-on-running-cluster-pods)
- [Install Dependencies](#install-dependencies)
- [Run Pipeline](#run-pipeline)
  - [Test pipeline on local Ray cluster](#test-pipeline-on-local-ray-cluster)
  - [Get access to Ray Dashboard](#get-access-to-ray-dashboard)
  - [Prepare runtime environment](#prepare-runtime-environment)
  - [Submit pipeline to RayCluster](#submit-pipeline-to-raycluster)
- [Cleanup](#cleanup)

## Overview

In this example we will run a simple pipeline with [Ray on Kubernetes](https://docs.ray.io/en/latest/cluster/kubernetes/index.html). Most of the examples usually run locally, but with a remote cluster additional configuration steps are required. Specifically, remote nodes (e.g. pods in k8s) do not know what dependencies should be pre-installed or what environment variables needs to be set. As you might have guessed, in a production environment you need to carefully plan how things are configured, e.g.:

- Ensure dependencies are pre-installed as part of containers (e.g. `haystack-ai` package as well as `ray-haystack`)
- Environment variables are set on remote nodes (pods)
- Workload is properly managed by requesting enough cluster resources (CPUs, GPUs, RAM)
- etc

We are not aiming here for a production ready setup, but rather focus on simple use case - connect to an existing KubeRay cluster and run Haystack pipeline with its environment dependencies.

## Project structure

```shell
.
├── README.md
├── pipeline.py # sample Haystack (Ray) pipeline
├── requirements.txt # declare dependencies, providing same version of Ray as it will be used in k8s
└── runtime_env.yaml # runtime environment definition with environment variables
```

> **Note**
> Sample pipeline is a modified version of the [Generating Structured Output with Loop-Based Auto-Correction](https://haystack.deepset.ai/tutorials/28_structured_output_with_loop) tutorial. You will need OpenAI API Key to run the example.

## Setup RayCluster

Lets setup a cluster using KubeRay operator. For that we will follow the steps from this [tutorial](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html).

### Step 1: Create a Kubernetes cluster

On my local I am using Docker Desktop with Kubernetes already enabled. You could use [kind](https://kind.sigs.k8s.io/) as suggested in the tutorial (e.g. `kind create cluster --image=kindest/node:v1.26.0`)

### Step 2: Deploy a KubeRay operator

```shell
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install both CRDs and KubeRay operator v1.2.1.
helm install kuberay-operator kuberay/kuberay-operator --version 1.2.1

# Confirm that the operator is running in the namespace `default`.
kubectl get pods
# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-7fbdbf8c89-pt8bk   1/1     Running   0          27s
```

> **Note**
> We are using version 1.2.1 of the helm chart (in the [tutorial](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html)
> the version is 1.1.1).

### Step 3: Deploy a RayCluster custom resource

With KubeRay operator installed we are ready to deploy RayCluster with one head and two worker pods having 2G RAM requirement:

```shell
# Deploy a sample RayCluster CR from the KubeRay Helm chart repo:
helm install raycluster kuberay/ray-cluster --version 1.2.1 \
    --set worker.replicas=2 \
    --set worker.resources.limits.memory="2G" \
    --set worker.resources.requests.memory="2G"

# Once the RayCluster CR has been created, you can view it by running:
kubectl get rayclusters

# The operator will then start your Ray cluster by creating head and worker pods.
# To view Ray cluster’s pods, run the following command:
kubectl get pods --selector=ray.io/cluster=raycluster-kuberay
```

> **Note**
> 2G RAM is requested for worker pods to make sure we have enough resources to run components from pipeline. You local k8s cluster should be able to support such payload (e.g. configure enough "Resources" in Docker Desktop).

### Check Ray and Python versions on running cluster pods

The output from the previous `kubectl get pods` command should give you one head and two worker pods running. Lets use the head pod to determine the version of both Ray and python running in the pod:

```shell
# Display the version of ray
kubectl exec -raycluster-kuberay-head-ldrdl -- ray --version
# Output: ray, version 2.34.0

# Display the version of ray
kubectl exec raycluster-kuberay-head-ldrdl -- python --version
# Output: Python 3.9.19
```

## Install Dependencies

The `requirements.txt` declares project's dependencies.
We already know what version of Python (`3.9.19`) and Ray (`2.34.0`) should be installed.
If there is python version mismatch, running pipeline with Job Submission SDK will fail.

I prefer using [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation) to manage python versions on my machine (e.g. `pyenv install 3.9.19`). You can set current version in your shell environment with `pyenv local 3.9.19`.

Below is `requirements.txt` file contents, having the matching version of the ray library:

```shell
ray[default]==2.34.0 # ray[default] will also install Ray dashboard
ray-haystack
```

Install dependencies by running:

```shell
# Check Python version beforehand
python --version
# Output: Python 3.9.19

# create a new virtual environment
python -m venv .venv

# activate a virtual environment
source .venv/bin/activate

pip install --upgrade pip

# install dependencies
pip install -r requirements.txt
```

## Run Pipeline

We will use the [Ray job submission SDK](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/quickstart.html#jobs-quickstart) to submit Ray jobs to the RayCluster via the Ray Dashboard port (8265 by default) where Ray listens for Job requests.

### Test pipeline on local Ray cluster

Lets make sure we can run the sample pipeline on your local (non-kubernetes) cluster:

```shell
# Provide your OpenAI API Key
export OPENAI_API_KEY="<OpenAI API Key>"

# Run pipeline using local Ray cluster
python pipeline.py
```

If above runs without issues we should be ready to submit pipeline execution to RayCluster in k8s.

### Get access to Ray Dashboard

Ray job submission requires address & port of the Ray Dashboard. We can expose the dashboard by using port forwarding:

```shell
# See if the service is running under the "raycluster-kuberay-head-svc" name
kubectl get service raycluster-kuberay-head-svc

# Execute this in a separate shell.
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265
```

If port forwarding worked as expected, open Ray Dashboard on your local by navigating to [http://localhost:8265](http://localhost:8265).

### Prepare runtime environment

So far we have installed RayCluster in k8s but it does not know what dependencies (e.g. `ray-haystack`) it should have available on each worker pod. If we submit pipeline at this point it wil fail because of missing dependencies. In production you will most certainly go with proper CI/CD process and docker image preparation to make sure certain dependencies are pre-installed for better performance. In our case we are going to instruct Ray to install dependencies on the fly during job submission with the help of [runtime environment](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments):

```yaml
working_dir: "." # files from this directory will be packaged & submitted to RayCluster
excludes:
  - ".venv/" # ignore local .venv folder
pip: "./requirements.txt" # install python dependencies on worker pods
env_vars:
  OPENAI_API_KEY: "You OpenAI API Key" # set environment variable in worker pods
```

Environment configuration is located in `./runtime_env.yaml`. Please update it with "Your OpenAI API Key". We ask Ray to install same dependencies as specified in the `requirements.txt`.

### Submit pipeline to RayCluster

We are finally ready to run the pipeline (while the port forwarding still working for Ray Dashboard):

```shell
ray job submit --address http://localhost:8265 --runtime-env runtime_env.yaml -- python pipeline.py
```

While job is running take a look at actors being created in [your local dashboard](http://localhost:8265/#/actors)

Hopefully you got back results with a message similar to:

```shell
------------------------------------------
Job 'raysubmit_syDEEfw2guXJ9u1k' succeeded
------------------------------------------
```

## Cleanup

```shell
# Uninstall the RayCluster Helm chart
helm uninstall raycluster

# Note that it may take several seconds for the Ray pods to be fully terminated.
# Confirm that the RayCluster's pods are gone by running
kubectl get pods

# Uninstall the KubeRay operator Helm chart
helm uninstall kuberay-operator

# Confirm that the KubeRay operator pod is gone by running
kubectl get pods
```
