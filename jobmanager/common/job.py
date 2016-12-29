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


def update_modified(sender, document, **kwargs):
    document.updated = datetime.utcnow()


def public_dict(d):
    """ if type(d) is dict:
        return dict((k, public_dict(v)) for k, v in d.items() if not k.startswith('_'))
    elif type(d) is list:
        return [public_dict(v) for v in d]
    else:
        return d
    """

    if isinstance(d, dict):
        safe_dict = dict((key, public_dict(value)) for key, value in d.items() if not key.startswith('_'))
        if '_cls' in d:
            safe_dict['type'] = d['_cls']
        return safe_dict
    if isinstance(d, list):
        return [public_dict(l) for l in d]

    return d


class SerializableQuerySet(mongoengine.QuerySet):

    def to_json(self):
        return tbx.text.render_json(self.as_pymongo())

    def to_safe_dict(self):
        return [f.to_safe_dict() for f in self]
        #return [public_dict(f) for f in self.as_pymongo()]


class BaseDocument(mongoengine.Document):

    created = mongoengine.DateTimeField(required=True, default=datetime.utcnow)
    updated = mongoengine.DateTimeField(default=datetime.utcnow)

    meta = {
        'ordering': ['+created'],
        'allow_inheritance': True,
        'queryset_class': SerializableQuerySet,
        'abstract': True,
        'strict': False,
        'indexes': [
            'created',
        ]
    }

    def __init__(self, *args, **values):
        super(BaseDocument, self).__init__(*args, **values)

    def to_json(self):
        return tbx.text.render_json(self.to_mongo())

    def to_safe_dict(self):
        return public_dict(self.to_mongo())


class NamedDocument(BaseDocument):

    uuid = mongoengine.StringField(required=True, default=tbx.text.random_short_slug, unique=True)
    name = mongoengine.StringField(required=True)
    module = mongoengine.StringField()

    meta = {
        'ordering': ['+created'],
        'allow_inheritance': True,
        'queryset_class': SerializableQuerySet,
        'abstract': True,
        'indexes': [
            'uuid',
            'created',
        ]
    }

    def __init__(self, *args, **values):
        super(NamedDocument, self).__init__(*args, **values)
        if not self.name:
            self.name = self.__class__.__name__ + ' ' + self.uuid

    def __repr__(self):
        return self.name


class Job(NamedDocument):

    meta = {
        'collection': 'jobs',
        'indexes': [
            'status',
            'created',
        ]
    }

    status = mongoengine.StringField(required=True, default="pending", choices=('new', 'pending', 'running', 'success', 'error'))
    status_text = mongoengine.StringField(required=True, default="")
    client_hostname = mongoengine.StringField(required=False)
    client_uuid = mongoengine.StringField(required=False)
    details = mongoengine.StringField(default="")
    completion = mongoengine.IntField(required=True, min_value=0, max_value=100, default=0)
    params = mongoengine.DictField(default={})
    started = mongoengine.DateTimeField()
    finished = mongoengine.DateTimeField()
    timeout = mongoengine.IntField(min_value=0, default=0)
    ttl = mongoengine.IntField(min_value=1, default=1)
    history = mongoengine.ListField(field=mongoengine.DictField(), default=[])

    def __str__(self):
        return "%s (%s)" % (self.name, self.status)

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
        return public_dict(self.to_mongo())

    @property
    def extra_log_arguments(self):
        return {
            'job_type': self.__class__.__name__,
            'job_uuid': self.uuid,
            'job_status': self.status,
            'client_hostname': self.client_hostname,
            'client_uuid': self.client_uuid,
        }

    def log_debug(self, text):
        logging.debug("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_info(self, text):
        logging.info("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_error(self, text):
        logging.error("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_exception(self, text):
        logging.exception("%s - %s" % (self, text), extra=self.extra_log_arguments)

mongoengine.signals.pre_save.connect(update_modified)


class JobTask(mongoengine.EmbeddedDocument):
    meta = {
        'abstract': True,
    }

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def job(self):
        return self._instance

    def __str__(self):
        return "%s > %s" % (self.job, self.name)

    def __repr__(self):
        return self.__str__()

    def log_debug(self, text):
        logging.debug("%s - %s" % (self, text))

    def log_info(self, text):
        logging.info("%s - %s" % (self, text))

    def log_warning(self, text):
        logging.warning("%s - %s" % (self, text))

    def log_error(self, text):
        logging.error("%s - %s" % (self, text))

    def log_exception(self, text):
        logging.exception("%s - %s" % (self, text))


class ExecuteJob(Job):
    command = mongoengine.StringField(required=True)
    output = mongoengine.StringField(default=None)

    def process(self):
        logging.info('ExecuteJob %s - Executing command...' % self.uuid)
        result = tbx.process.execute(self.command, return_output=True)
        logging.info(result)
        self.output = result


class WaitJob(Job):
    duration = mongoengine.IntField(required=True)
    fail_ratio = mongoengine.FloatField(default=0.0, min_value=0.0, max_value=1.0)

    def process(self):
        import time
        import random
        for i in range(0, self.duration):
            time.sleep(1)
            self.update_progress(i / self.duration*100.0, "Waiting %d seconds over %d (%0.1f%%)" % (i, self.duration, i / self.duration*100.0))
            if random.random() < self.fail_ratio:
                raise Exception('Arbitrary fail ratio triggered. Exiting job with Exception raised.')

class Client(NamedDocument):
    meta = {
        'ordering': ['-updated'],
        'max_documents': 10000,
        'queryset_class': SerializableQuerySet,
        'indexes': [
            'uuid',
            'created',
            'hostname'
        ]
    }
    hostname = mongoengine.StringField(required=True)
    pid = mongoengine.IntField(required=True)
    job_types = mongoengine.ListField(required=True, field=mongoengine.StringField(), default=[])
    pool_size = mongoengine.IntField(required=True, min_value=1)
    hostname = mongoengine.StringField(required=True)
    platform = mongoengine.DictField()
    boot_time = mongoengine.DateTimeField()
    python_version = mongoengine.StringField()
    python_packages = mongoengine.ListField(field=mongoengine.StringField())

    def history(self, offset=0, limit=30, step=0):
        step_filter = {}
        if step and step>1:
            step_filter = {'index__mod':(step,0)}
        statuses = ClientStatus.objects(client=self, **step_filter).order_by('-created')[offset:limit]
        return [s.to_safe_dict(with_client=False) for s in statuses]

    def alive(self):
        recent_count = ClientStatus.objects(client=self, created__gte=datetime.utcnow() - timedelta(minutes=0.5)).count()
        return recent_count > 0

    def last_seen_alive(self):
        last_status = ClientStatus.objects(client=self).order_by('-created').only('created').first()
        if not last_status or not last_status.created:
            return None
        return last_status.created

    def to_safe_dict(self, alive=False, with_history=False, offset=0, limit=30, step=0):
        r = super(Client, self).to_safe_dict()
        if alive:
            r['alive'] = self.alive()
            r['last_seen_alive'] = self.last_seen_alive()
        if with_history:
            r['history'] = self.history(offset=offset, limit=limit, step=step)
        return r


class ClientStatus(BaseDocument):

    meta = {
        'ordering': ['-created'],
        'max_documents': 200000,
        'max_size': 200000000,
        'queryset_class': SerializableQuerySet,
        'indexes': [
            'created',
            'client'
        ]
    }
    client = mongoengine.CachedReferenceField(Client, fields=['uuid'], reverse_delete_rule=mongoengine.CASCADE)
    index = mongoengine.LongField(required=True, default=0)
    current_jobs = mongoengine.ListField(field=mongoengine.CachedReferenceField(Job, fields=['uuid', '_cls'], auto_sync=True), default=[])
    system_status = mongoengine.DictField(default={})
    updated = None

    def to_safe_dict(self, with_client=True):
        r = super(ClientStatus, self).to_safe_dict()
        if not with_client:
            del r['client']
            del r['type']
        return r