from uuid import uuid4
import time

from gcp_pal import CloudScheduler


def test_cloud_scheduler():
    success = {}

    job_name = f"test-job-{uuid4()}"

    try:

        success[0] = not job_name in CloudScheduler().ls()
        success[1] = not CloudScheduler(job_name).exists()

        CloudScheduler(job_name).create(
            schedule="*/1 * * * *", target="http://example.com"
        )

        got = CloudScheduler(job_name).get()
        success[2] = got.name.endswith(job_name)
        success[3] = got.http_target.uri == "http://example.com/"
        success[4] = got.schedule == "*/1 * * * *"
        success[5] = got.time_zone == "UTC"

        success[6] = job_name in CloudScheduler().ls()
        time.sleep(5)
        success[7] = CloudScheduler(job_name).exists()
        time.sleep(5)

        success[8] = CloudScheduler(job_name).state() == "ENABLED"
        time.sleep(2)
        success[9] = CloudScheduler(job_name).status() == "Has not run yet"
        time.sleep(2)

        CloudScheduler(job_name).run()
        time.sleep(5)

        success[10] = CloudScheduler(job_name).state() == "ENABLED"
        time.sleep(2)
        success[11] = CloudScheduler(job_name).status() == "Success"
        time.sleep(2)

        CloudScheduler(job_name).pause()
        time.sleep(2)

        success[12] = CloudScheduler(job_name).state() == "PAUSED"
        time.sleep(2)
        success[13] = CloudScheduler(job_name).status() == "Success"
        time.sleep(2)

        CloudScheduler(job_name).resume()
        time.sleep(2)

        success[14] = CloudScheduler(job_name).state() == "ENABLED"
        time.sleep(2)
        success[15] = CloudScheduler(job_name).status() == "Has not run yet"
        time.sleep(2)

        CloudScheduler(job_name).delete()
        time.sleep(2)

        success[16] = not job_name in CloudScheduler().ls()
        time.sleep(2)
        success[17] = not CloudScheduler(job_name).exists()
        time.sleep(2)

    except Exception as e:
        if "429 Quota exceeded for quota metric" in str(e) or "429 Quota limit" in str(
            e
        ):
            print("Could not finish test_scheduler due to quota exceeded error.")
        else:
            raise e

    failed = [k for k, v in success.items() if not v]

    assert not failed
