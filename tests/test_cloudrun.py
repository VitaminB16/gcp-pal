from uuid import uuid4

from gcp_pal import CloudRun


def test_cloudrun():
    success = {}
    run_name = f"test-run-{uuid4()}"
    success[0] = run_name not in CloudRun().ls()
    success[1] = not CloudRun(run_name).exists()

    CloudRun(run_name).create(path="samples/cloud_run", memory=512)
    success[2] = CloudRun(run_name).exists()
    success[3] = run_name in CloudRun().ls()
    success[4] = CloudRun(run_name).status() == "Active"
    success[5] = CloudRun(run_name).state() == "Active"

    output = CloudRun(run_name).call(data={"data": 16})
    success[5] = output == {"result": 4.0}

    got = CloudRun(run_name).get()
    success[6] = got.name.startswith("projects/")
    success[7] = got.name.endswith(f"services/{run_name}")
    success[8] = got.template.containers[0].resources.limits["memory"] == "512Mi"

    CloudRun(run_name).delete()

    success[9] = not CloudRun(run_name).exists()
    success[10] = run_name not in CloudRun().ls()

    failed = [k for k, v in success.items() if not v]
    assert not failed
