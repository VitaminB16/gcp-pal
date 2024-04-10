from gcp_tools import CloudFunctions


def test_cloudfunctions_gen1():
    success = {}
    fun_name = "test_function_1"
    ls_before = CloudFunctions().ls()
    cloud_fun = CloudFunctions(fun_name)
    cloud_fun.delete(errors="ignore")  # Make sure it is not present
    success[0] = not cloud_fun.exists()
    response = cloud_fun.deploy(
        source="samples/cloud_function",
        entry_point="entry_point",
        runtime="python310",
        environment=1,
    )
    success[1] = cloud_fun.exists()
    ls_during = CloudFunctions().ls()
    success[2] = fun_name in set(ls_during)
    output = cloud_fun.call(data={"data": 4})
    success[3] = output == {"result": 2}
    success[4] = cloud_fun.state() == "ACTIVE"
    cloud_fun_info = cloud_fun.get()
    success[5] = cloud_fun_info.build_config.entry_point == "entry_point"
    success[6] = cloud_fun_info.environment.name == "GEN_1"

    cloud_fun.delete()
    success[7] = not cloud_fun.exists()
    ls_after = CloudFunctions().ls()
    success[8] = fun_name not in set(ls_after)

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_cloudfunctions_gen2():
    success = {}
    fun_name = "test_function_2"
    ls_before = CloudFunctions().ls()
    cloud_fun = CloudFunctions(fun_name)
    cloud_fun.delete(errors="ignore")  # Make sure it is not present
    success[0] = not cloud_fun.exists()
    response = cloud_fun.deploy(
        source="samples/cloud_function",
        entry_point="entry_point",
        runtime="python310",
    )
    success[1] = cloud_fun.exists()
    ls_during = CloudFunctions().ls()
    success[2] = fun_name in set(ls_during)
    output = cloud_fun.call(data={"data": 4})
    success[3] = output == {"result": 2}
    success[4] = cloud_fun.state() == "ACTIVE"
    cloud_fun_info = cloud_fun.get()
    success[5] = cloud_fun_info.build_config.entry_point == "entry_point"
    success[6] = cloud_fun_info.environment.name == "GEN_2"

    cloud_fun.delete()
    success[7] = not cloud_fun.exists()
    ls_after = CloudFunctions().ls()
    success[8] = fun_name not in set(ls_after)

    failed = [k for k, v in success.items() if not v]

    assert not failed
