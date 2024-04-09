from gcp_tools import CloudFunctions


def test_cloudfunctions():
    success = {}
    cloud_fun = CloudFunctions("sample1")
    success[0] = not cloud_fun.exists()
    response = cloud_fun.deploy(
        source="samples/cloud_function",
        entry_point="entry_point",
        runtime="python310",
    )
    success[1] = cloud_fun.exists()
    response = cloud_fun.call(data={"data": 4})
    success[2] = response.status_code == 200
    success[3] = response.json() == {"result": 2}

    failed = [k for k, v in success.items() if not v]

    assert not failed
