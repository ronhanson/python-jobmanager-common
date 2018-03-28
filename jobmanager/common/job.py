#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 nu
"""
(c) 2015 Ronan Delacroix
Job Manager Job Abstract Class
:author: Ronan Delacroix
"""
import os
import logging
import mongoengine
import mongoengine.signals
import tbx
import tbx.process
import tbx.service
import tbx.log
import tbx.text
import uuid as UUID
import traceback
import tempfile
import shutil
from datetime import datetime, timedelta
import jobmanager.common as common
from .host import Host
from tbx.code import cached_property


job_status_to_icon = {
    'new': "\u23F8",
    'pending': "\u23F8",
    'running': "\u25B6",
    'success': "\u2605",
    'error': "\u2716"
}


class Job(common.NamedDocument, common.Runnable, common.LogProxy, common.AutoDocumentable):

    meta = {
        'collection': 'jobs',
        'indexes': [
            'status',
            'created',
        ]
    }

    status_text = mongoengine.StringField(required=True, default="")
    hostname = mongoengine.StringField()
    completion = mongoengine.IntField(required=True, min_value=0, max_value=100, default=0)
    timeout = mongoengine.IntField(min_value=0, default=43200)  # 12 hours
    ttl = mongoengine.IntField(min_value=1, default=1)
    history = mongoengine.ListField(field=mongoengine.DictField(), default=[])

    def __str__(self):
        return "%s %s" % (self.name, job_status_to_icon.get(self.status, self.status))

    @classmethod
    def default_slot_amount(cls):
        """
        Returns the default amount of job that can be run at the same time on the same machine/client.
        Override and set to math.inf for no limiting amount.
        You can base that how much CPU cores you have or anything else.
        Default is 1 job at a time by default.
        """
        return 1

    @cached_property
    def extra_log_arguments(self):
        return {
            'job_type': self.__class__.__name__,
            'job_uuid': self.uuid,
            'job_status': self.status,
        }

    def update_status(self, completion=None, text=None):
        if text:
            self.status_text = text

        if completion:
            self.completion = completion

        log = self.log_info
        if self.status == 'error':
            log = self.log_error

        log("Progress update : {progress:5.1f}% - {message}".format(
            progress=self.completion,
            message=self.status_text
        ))

        self.update(
            add_to_set__history={'t': datetime.utcnow(), 'm': self.status_text, 'c': self.completion, 's': self.status},
            status=self.status,
            details=self.details,
            completion=self.completion,
            status_text=self.status_text,
            started=self.started,
            finished=self.finished
        )

    def update_progress(self, completion, text=None):
        self.update_status(completion=completion, text=text)

    def save_as_successful(self, text="Job Successful"):
        self.update_status(100, text=text)

    def save_as_error(self, text="Job Error"):
        self.update_status(text=text)


mongoengine.signals.pre_save.connect(common.update_modified)


class JobTask(mongoengine.EmbeddedDocument, common.Runnable, common.LogProxy, common.AutoDocumentable):
    meta = {
        'abstract': True,
    }

    status = mongoengine.StringField(required=True, default="pending",
                                     choices=('new', 'pending', 'running', 'success', 'error'))
    details = mongoengine.StringField(required=False)

    @property
    def job(self):
        if isinstance(self._instance, JobTask):
            return self._instance.job
        return self._instance

    @cached_property
    def extra_log_arguments(self):
        extra_log_arguments = {}
        if isinstance(self.job, common.LogProxy):
            extra_log_arguments = self.job.extra_log_arguments
        extra_log_arguments['task'] = self.name
        return extra_log_arguments

    def __str__(self):
        return "%s > %s" % (self.job, self.name)

    def get_hash(self):
        import base64
        import hashlib
        return base64.b64encode(
            hashlib.sha1(mongoengine.EmbeddedDocument.to_json(self, sort_keys=True).encode()).digest()).decode().strip('=').replace("+", "-")

    def update_status(self, completion=None, text=None):
        #TODO : Review this part // Completion between tasks and jobs is not clear.
        if text:
            self.job.status_text = text

        if completion:
            self.job.completion = completion

        log = self.log_info
        if self.status == 'error':
            log = self.log_error

        log("Progress update : {progress:5.1f}% - {message}".format(
            progress=self.job.completion,
            message=text
        ))

    def update_progress(self, completion, text=None):
        self.update_status(completion=completion, text=text)


def make_job(job_name, **kwargs):
    """
    Decorator to create a Job from a function.
    Give a job name and add extra fields to the job.

        @make_job("ExecuteDecJob",
                  command=mongoengine.StringField(required=True),
                  output=mongoengine.StringField(default=None))
        def execute(job: Job):
            job.log_info('ExecuteJob %s - Executing command...' % job.uuid)
            result = subprocess.run(job.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            job.output = result.stdout.decode('utf-8') + " " + result.stderr.decode('utf-8')

    """
    def wraps(func):
        kwargs['process'] = func
        job = type(job_name, (Job,), kwargs)
        globals()[job_name] = job
        return job
    return wraps
