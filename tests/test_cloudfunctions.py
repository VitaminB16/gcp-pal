from gcp_tools import CloudFunctions


def test_cloudfunctions():
    success = {}
    cloud_fun = CloudFunctions("sample1")
    response = cloud_fun.deploy(
        source="samples/cloud_function",
        entry_point="entry_point",
        runtime="python39",
        memory="256MB",
        timeout="60s",
        trigger_http=True,
    )
    success[0] = cloud_fun.exists()

    failed = [k for k, v in success.items() if not v]

    assert not failed
