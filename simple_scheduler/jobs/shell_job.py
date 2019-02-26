"""A job to run executable programs."""

from shell import shell
from ndscheduler import job


class ShellJob(job.JobBase):
    @classmethod
    def meta_info(cls):
        return {
            "job_class_string": "%s.%s" % (cls.__module__, cls.__name__),
            "notes": (
                "This will run an executable program. You can specify all params "
                "in a single string."
            ),
            "arguments": {"type": "string", "description": "Executable path"},
            "example_arguments":
                '["/usr/local/my_program --file /tmp/abc --mode safe"]',
        }

    def run(self, shell_cmd, *args, **kwargs):
        print("Running shell job '{}'".format(shell_cmd))
        cmd = shell(shell_cmd)
        return {"stdout": cmd.output(), "errors": cmd.errors()}


if __name__ == "__main__":
    # You can easily test this job here
    job = ShellJob.create_test_instance()
    job.run("ls -l")
