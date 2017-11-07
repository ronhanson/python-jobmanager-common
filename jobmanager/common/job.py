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
from datetime import datetime, timedelta
import jobmanager.common as common
from .host import Host


job_status_to_icon = {
    'new': "\u23F8",
    'pending': "\u23F8",
    'running': "\u25B6",
    'success': "\u2605",
    'error': "\u2716"
}


class Job(common.NamedDocument):

    meta = {
        'collection': 'jobs',
        'indexes': [
            'status',
            'created',
        ]
    }

    hostname = mongoengine.StringField()
    status = mongoengine.StringField(required=True, default="pending", choices=('new', 'pending', 'running', 'success', 'error'))
    status_text = mongoengine.StringField(required=True, default="")
    details = mongoengine.StringField(default="")
    completion = mongoengine.IntField(required=True, min_value=0, max_value=100, default=0)
    started = mongoengine.DateTimeField()
    finished = mongoengine.DateTimeField()
    timeout = mongoengine.IntField(min_value=0, default=0)
    ttl = mongoengine.IntField(min_value=1, default=1)
    history = mongoengine.ListField(field=mongoengine.DictField(), default=[])

    def __str__(self):
        return "%s %s" % (self.name, job_status_to_icon.get(self.status, self.status))

    def __repr__(self):
        return self.__str__()

    def process(self):
        raise NotImplementedError('The process method shall be subclassed to define the job processing.')

    def run(self):
        self.started = datetime.utcnow()
        self.update_status(status='running', completion=1, text='Running job')
        try:
            self.log_debug("Launching job process...")
            self.process()
        except Exception as e:
            self.log_exception(e)
            self.details = "Exception : %s" % str(traceback.format_exc())
            self.save_as_error(text="Error while running job (%s)." % e)
            raise e
        else:
            self.save_as_successful()

    def update_status(self, status=None, completion=None, text=None):
        if status:
            self.status = status
            if status in ['success', 'error']:
                self.finished = datetime.utcnow()
        if text:
            self.status_text = text
        if completion:
            self.completion = completion
        log = self.log_info
        if self.status == 'error':
            log = self.log_error
        if status:
            log("Status update : {status} - {progress:5.1f}% - {message}".format(
                status=self.status,
                progress=self.completion,
                message=self.status_text
            ))
        else:
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

    def save_as_successful(self, text='Job Successful'):
        self.update_status('success', completion=100, text=text)

    def save_as_error(self, text='Job Error'):
        self.update_status('error', text=text)

    def to_safe_dict(self):
        return common.public_dict(self.to_mongo())

    @property
    def extra_log_arguments(self):
        if not hasattr(self, '__extra_log_arguments'):
            self.__extra_log_arguments = {
                'job_type': self.__class__.__name__,
                'job_uuid': self.uuid,
                'job_status': self.status,
            }
        return self.__extra_log_arguments

    def log_debug(self, text):
        logging.debug("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_info(self, text):
        logging.info("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_warning(self, text):
        logging.warning("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_error(self, text):
        logging.error("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_exception(self, text):
        logging.exception("%s - %s" % (self, text), extra=self.extra_log_arguments)

mongoengine.signals.pre_save.connect(common.update_modified)


class JobTask(mongoengine.EmbeddedDocument):
    meta = {
        'abstract': True,
    }

    status = mongoengine.StringField(required=True, default="pending",
                                     choices=('new', 'pending', 'running', 'success', 'error'))
    details = mongoengine.StringField(required=False)

    @property
    def extra_log_arguments(self):
        if not hasattr(self, '__extra_log_arguments'):
            self.__extra_log_arguments = self.job.extra_log_arguments
            self.__extra_log_arguments['task'] = self.name
        return self.__extra_log_arguments

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def parent(self):
        return self._instance

    def __str__(self):
        return "%s > %s" % (self.parent, self.name)

    def __repr__(self):
        return self.__str__()

    def process(self, *args, **kwargs):
        raise NotImplementedError('The process method shall be implemented to define the task processing.')

    def run(self, *args, **kwargs):
        self.status = 'running'
        self.save()
        try:
            self.log_debug("Launching task process...")
            self.process(*args, **kwargs)
        except Exception as e:
            self.status = 'error'
            self.details = "Error while running task (%s)." % e
            raise e
        else:
            self.status = 'success'
        self.save()

    def log_debug(self, text):
        logging.debug("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_info(self, text):
        logging.info("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_warning(self, text):
        logging.warning("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_error(self, text):
        logging.error("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_exception(self, text):
        logging.exception("%s - %s" % (self, text), extra=self.extra_log_arguments)
