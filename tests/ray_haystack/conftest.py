import logging

import pytest
import ray

logger = logging.getLogger("conftest")


@pytest.fixture(scope="module", autouse=True)
def ray_session():
    logger.info("Start local Ray cluster...")
    ray.init()

    yield

    logger.info("Shutting down local Ray cluster...")
    ray.shutdown()
