"""
Example module to demonstrate how the CloudRun module can be used for
deployment and invocation of Google Cloud Run services.
"""


def example_cloud_run():
    from gcp_pal import CloudRun

    service_name = f"example-service-123"
    codebase_path = "samples/cloud_run"

    # Build the Docker image,
    # push it to gcr.io/{project_id}/{service_name}:random_tag,
    # and deploy the service
    CloudRun(service_name).deploy(path=codebase_path)

    # List all services
    services = CloudRun().ls()
    print(services)

    # Invoke the service with payload
    response = CloudRun(service_name).call({"data": 16})
    print(response)

    # Delete the service
    CloudRun(service_name).delete()

    # List all services
    services = CloudRun().ls()
    print(services)


if __name__ == "__main__":
    example_cloud_run()
