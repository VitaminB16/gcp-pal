from uuid import uuid4
import time

from gcp_tools import CloudScheduler


def test_cloud_scheduler():
    success = {}

    job_name = f"test-job-{uuid4()}"

    success[0] = not job_name in CloudScheduler().ls()
    success[1] = not CloudScheduler(job_name).exists()

    CloudScheduler(job_name).create(schedule="*/1 * * * *", target="http://example.com")

    got = CloudScheduler(job_name).get()
    success[2] = got.name.endswith(job_name)
    success[3] = got.http_target.uri == "http://example.com/"
    success[4] = got.schedule == "*/1 * * * *"
    success[5] = got.time_zone == "UTC"

    success[6] = job_name in CloudScheduler().ls()
    success[7] = CloudScheduler(job_name).exists()

    success[8] = CloudScheduler(job_name).state() == "ENABLED"
    success[9] = CloudScheduler(job_name).status() == "Has not run yet"

    CloudScheduler(job_name).run()
    time.sleep(5)

    success[10] = CloudScheduler(job_name).state() == "ENABLED"
    success[11] = CloudScheduler(job_name).status() == "Success"

    CloudScheduler(job_name).pause()

    success[12] = CloudScheduler(job_name).state() == "PAUSED"
    success[13] = CloudScheduler(job_name).status() == "Success"

    CloudScheduler(job_name).resume()

    success[14] = CloudScheduler(job_name).state() == "ENABLED"
    success[15] = CloudScheduler(job_name).status() == "Has not run yet"

    CloudScheduler(job_name).delete()

    success[16] = not job_name in CloudScheduler().ls()
    success[17] = not CloudScheduler(job_name).exists()

    failed = [k for k, v in success.items() if not v]

    assert not failed
