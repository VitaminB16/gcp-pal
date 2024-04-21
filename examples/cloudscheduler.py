"""
Example module to demonstrate how the CloudScheduler module can be used for
creating and managing Scheduler jobs.
"""


def example_scheduler():
    from gcp_pal import CloudScheduler

    job_name = f"example-job-123"
    schedule = "*/2 * * * *"  # Run every 2 minutes

    # Create a job
    CloudScheduler(job_name).create(schedule=schedule, target="http://example.com")

    # List all jobs
    print(CloudScheduler().ls())

    # Check the status of the job
    print(CloudScheduler(job_name).status())

    # Run the job
    CloudScheduler(job_name).run()

    # Pause the job
    CloudScheduler(job_name).pause()

    # Resume the job
    CloudScheduler(job_name).resume()

    # Delete the job
    CloudScheduler(job_name).delete()

    # List all jobs
    print(CloudScheduler().ls())


if __name__ == "__main__":
    example_scheduler()
