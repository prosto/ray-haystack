from ray_haystack.ray_pipeline_settings import RayPipelineSettingsWrapper


def test_common_settings_merge_with_default():
    default_actor_options = RayPipelineSettingsWrapper.DEFAULT_COMMON_SETTINGS["actor_options"]

    settings_wrapper = RayPipelineSettingsWrapper(
        {
            "common": {
                "actor_options": {
                    "max_concurrency": 2,
                    "lifetime": "detached",  # override default
                }
            }
        }
    )

    assert settings_wrapper.common_settings == {
        "actor_options": {
            "max_concurrency": 2,
            "enable_task_events": default_actor_options["enable_task_events"],
            "lifetime": "detached",
        },
    }


def test_component_settings():
    default_actor_options = RayPipelineSettingsWrapper.DEFAULT_COMMON_SETTINGS["actor_options"]

    settings_wrapper = RayPipelineSettingsWrapper(
        {
            "common": {
                "actor_options": {
                    "num_cpus": 2,
                    "namespace": "haystack",
                    "get_if_exists": False,
                    "runtime_env": {
                        "env_vars": {
                            "ENV_1": "VAL_1",
                            "ENV_2": "VAL_2",
                        }
                    },
                },
            },
            "components": {
                "actor_options": {
                    "num_cpus": 3,
                    "runtime_env": {
                        "env_vars": {
                            "ENV_2": "components_VAL_2",
                            "ENV_3": "VAL_3",
                            "ENV_4": "VAL_4",
                        }
                    },
                },
                "per_component": {
                    "comp_1": {
                        "actor_options": {
                            "name": "comp_1_actor",
                            "get_if_exists": True,
                            "runtime_env": {
                                "env_vars": {
                                    "ENV_3": "comp_1_VAL_3",
                                    "ENV_5": "VAL_5",
                                }
                            },
                        }
                    }
                },
            },
        }
    )

    component_settings = settings_wrapper.get_component_settings("comp_1")

    assert component_settings == {
        "actor_options": {
            "enable_task_events": default_actor_options["enable_task_events"],  # common (default)
            "lifetime": default_actor_options["lifetime"],  # common (default)
            "num_cpus": 3,  # common for all components
            "namespace": "haystack",
            "name": "comp_1_actor",  # comp_1 specific
            "get_if_exists": True,  # comp_1 specific
            "runtime_env": {
                "env_vars": {
                    "ENV_1": "VAL_1",  # common for all components
                    "ENV_2": "components_VAL_2",  # common for all components
                    "ENV_3": "comp_1_VAL_3",  # comp_1 specific
                    "ENV_4": "VAL_4",  # common for all components
                    "ENV_5": "VAL_5",  # comp_1 specific
                },
            },
        },
        "middleware": {},
    }
