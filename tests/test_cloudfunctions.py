from gcp_tools import CloudFunctions


def test_cloudfunctions():
    success = {}
    cloud_fun = CloudFunctions("test_function_0")
    success[0] = not cloud_fun.exists()
    response = cloud_fun.deploy(
        source="samples/cloud_function",
        entry_point="entry_point",
        runtime="python310",
    )
    success[1] = cloud_fun.exists()
    output = cloud_fun.call(data={"data": 4})
    success[2] = output == {"result": 2}
    cloud_fun.delete()
    success[3] = not cloud_fun.exists()

    failed = [k for k, v in success.items() if not v]

    assert not failed
