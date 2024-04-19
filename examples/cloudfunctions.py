"""
Example module to demonstrate how the CloudFunctions module can be used for
deployment and invocation of Google Cloud Functions.
"""


def example_cloud_functions():
    from gcp_tools import CloudFunctions

    function_name = f"example-function-123"
    codebase_path = "samples/cloud_function"

    # Deploy the function
    CloudFunctions(function_name).deploy(path=codebase_path, entry_point="entry_point")

    # List all functions
    functions = CloudFunctions().ls()
    print(functions)

    # Invoke the function with payload
    response = CloudFunctions(function_name).call({"data": 16})
    print(response)

    # Delete the function
    CloudFunctions(function_name).delete()

    # List all functions
    functions = CloudFunctions().ls()
    print(functions)


if __name__ == "__main__":
    example_cloud_functions()
