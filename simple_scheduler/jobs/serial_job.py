"""A job to run executable programs."""

import json

from ndscheduler import job
from ndscheduler import utils
from ndscheduler import constants
from ndscheduler.core import scheduler_manager


class SerialExecutionException(Exception):
    pass


class SerialJob(job.JobBase):
    @classmethod
    def meta_info(cls):
        return {
            "job_class_string": "%s.%s" % (cls.__module__, cls.__name__),
            "notes": ("This will run list of tasks consequently"),
            "arguments": [{"type": "dict", "description": "Job class path and it params"}],
            "example_arguments":
                '[{"class": "simple_scheduler.jobs.sample_job.AwesomeJob", "args": {"foo": "bar", "baz": [1, 2, 3]}},'
                 '{"class": "simple_scheduler.jobs.shell_job.ShellJob", "args": "wc -l /tmp/foo.txt"}]',
        }

    def run(self, *args, **kwargs):
        scheduler = scheduler_manager.SchedulerManager.get_instance()
        datastore = scheduler.get_datastore()

        res = []

        for job_no, job_info in enumerate(args):
            job = utils.import_from_path(job_info["class"])
            execution_id = utils.generate_uuid()

            job_id = "{}-{}".format(self.job_id, job_no)
            datastore.add_execution(
                execution_id,
                job_id,
                constants.EXECUTION_STATUS_SCHEDULED,
                description="Subtask {}: {}".format(
                    job_no, job.get_scheduled_description()
                ),
            )

            job.run_job(self.job_id, execution_id, *job_info.get("args", []))
            exec_state = datastore.get_execution(execution_id)

            if exec_state["state"] == constants.EXECUTION_STATUS_SCHEDULED_ERROR:
                raise SerialExecutionException(
                    "Job #{} failed with result {}".format(job_no, exec_state["result"])
                )

            res.append(json.loads(exec_state["result"]))

        return res
