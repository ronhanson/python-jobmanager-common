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
import datetime
import tbx
import tbx.process
import tbx.service
import tbx.log
import tbx.text
import uuid as UUID
import traceback


def update_modified(sender, document, **kwargs):
    document.updated = datetime.datetime.utcnow()


def public_dict(d):
    safe_dict = dict((key, value) for key, value in d.items() if not key.startswith('_'))
    safe_dict['type'] = d['_cls']
    return safe_dict


class SerializableQuerySet(mongoengine.QuerySet):

    def to_json(self):
        return tbx.text.render_json(self.as_pymongo())

    def to_safe_dict(self):
        return [public_dict(f) for f in self.as_pymongo()]


class BaseDocument(mongoengine.Document):

    uuid = mongoengine.StringField(required=True, default=tbx.text.random_short_slug, unique=True)
    name = mongoengine.StringField(required=True)
    created = mongoengine.DateTimeField(required=True, default=datetime.datetime.utcnow)
    updated = mongoengine.DateTimeField(required=True)
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
        super(BaseDocument, self).__init__(*args, **values)
        if not self.name:
            self.name = self.__class__.__name__

    def __repr__(self):
        return "%s %s" % (self.name, self.uuid)

    def to_json(self):
        return tbx.text.render_json(self.to_mongo())

    def to_safe_dict(self):
        return public_dict(self.to_mongo())


class Job(BaseDocument):

    meta = {
        'collection': 'jobs',
        'indexes': [
            'status',
            'created',
        ]
    }

    status = mongoengine.StringField(required=True, default="pending", choices=('new', 'pending', 'running', 'success', 'error'))
    status_text = mongoengine.StringField(required=True, default="")
    details = mongoengine.StringField(default="")
    completion = mongoengine.IntField(required=True, min_value=0, max_value=100, default=0)
    params = mongoengine.DictField(default={})
    started = mongoengine.DateTimeField()
    finished = mongoengine.DateTimeField()
    history = mongoengine.ListField(field=mongoengine.DictField(), default=[])

    def __repr__(self):
        return "%s %s (%s)" % (self.name, self.id, self.status)

    def run(self):
        self.started = datetime.datetime.utcnow()
        self.update_status('running', completion=1, text='Running job')
        try:
            self.log_debug("Launching job process...")
            self.process()
        except Exception as e:
            self.details = "Exception : %s" % str(traceback.format_exc())
            self.save_as_error(text="Error while running job (%s)." % e)
        else:
            self.save_as_successful()

    def update_status(self, status=None, completion=None, text=None):
        if status:
            self.status = status
            if status in ['success', 'error']:
                self.finished = datetime.datetime.utcnow()
        if text:
            self.status_text = text
        if completion:
            self.completion = completion
        log = self.log_info
        if self.status == 'error':
            log = self.log_error
        if status:
            log("Status update : {status} - {progress:5.1f}% - {message}".format(self.status, str(self.completion), self.status_text))
        else:
            log("Progress update : {progress:5.1f}% - {message}".format(str(self.completion), self.status_text))
        self.update(
            add_to_set__history={'t': datetime.datetime.now(), 'm': self.status_text, 'c': self.completion, 's': self.status},
            status=self.status,
            completion=self.completion,
            status_text=self.status_text

        )

    def update_progress(self, completion, text=None):
        self.update_status(completion=completion, text=text)

    def save_as_successful(self, text='Job Successful'):
        self.update_status('success', completion=100, text=text)

    def save_as_error(self, text='Job Error'):
        self.update_status('error', text=text)

    def to_safe_dict(self):
        d = public_dict(self.to_mongo())
        d['type'] = self._cls
        return d

    def log_debug(self, text):
        logging.debug("%s - %s" % (self, text))

    def log_info(self, text):
        logging.info("%s - %s" % (self, text))

    def log_error(self, text):
        logging.error("%s - %s" % (self, text))

mongoengine.signals.pre_save.connect(update_modified)


class ExecuteJob(Job):
    command = mongoengine.StringField(required=True)
    output = mongoengine.StringField(default=None)

    def process(self):
        logging.info('Executing job ExecuteJob...')
        result = tbx.process.execute(self.command, return_output=True)
        logging.info(result)
        self.output = result

