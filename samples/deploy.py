import sys
from gcp_pal import CloudFunctions, CloudRun

if __name__ == "__main__":
    args = sys.argv[1:]
    if not isinstance(args, list):
        args = [args]
    if "cloud_function" in args:
        print("Deploying cloud_function")
        name = "test_function_1"
        CloudFunctions(name).deploy(
            path="cloud_function",
            entry_point="entry_point",
            runtime="python310",
            environment=2,
            env_vars_file="cloud_function/env.yaml",
        )
    if "cloud_run" in args:
        print("Deploying cloud_run")
        name = "test_service_1"
        CloudRun(name).create(
            path="cloud_run",
            memory=512,
        )
