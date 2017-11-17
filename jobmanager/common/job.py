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
from tbx.code import cached_property


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

    @property
    def type(self):
        return self.__class__.__name__

    def to_json(self):
        return tbx.text.render_json(self.to_mongo())

    def to_safe_dict(self):
        return public_dict(self.to_mongo())

    def get_hash(self):
        import base64
        import hashlib
        return base64.b64encode(hashlib.sha1(mongoengine.Document.to_json(self, sort_keys=True).encode()).digest()).decode().strip('=').replace("+", "-")


class NamedDocument(BaseDocument):

    uuid = mongoengine.StringField(required=True, default=tbx.text.random_short_slug, unique=True)
    name = mongoengine.StringField(required=True)

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
            self.name = self.type + ' ' + self.uuid

    def __repr__(self):
        return self.name


class LogProxy:

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def extra_log_arguments(self):
        return {}

    def log(self, text, method=logging.info):
        return method("%s - %s" % (self, text), extra=self.extra_log_arguments)

    def log_debug(self, text):
        return self.log(text, logging.debug)

    def log_info(self, text):
        return self.log(text, logging.info)

    def log_warning(self, text):
        return self.log(text, logging.warning)

    def log_error(self, text):
        return self.log(text, logging.error)

    def log_exception(self, text):
        return self.log(text, logging.exception)


class TempFolderProxy:

    @property
    def temp_folders(self):
        try:
            return self.__temp_folders
        except AttributeError:
            self.__temp_folders = []
        return self.__temp_folders

    def get_new_temp_folder(self, prefix=None):
        if prefix:
            prefix = prefix + '_'
        temp_folder = tempfile.mkdtemp(prefix=prefix)
        self.temp_folders.append(temp_folder)
        return temp_folder

    def get_named_temp_folder(self, name):
        temp_folder = os.path.join(tempfile.gettempdir(), name)
        os.makedirs(temp_folder, exist_ok=True)
        self.temp_folders.append(temp_folder)
        return temp_folder

    def clean_temp(self):

        if isinstance(self, LogProxy):
            log_func = self.log_debug
        else:
            def do_nothing(*args, **kwargs):
                pass
            log_func = do_nothing

        if not self.temp_folders:
            return

        log_func("Cleaning temporary folders.")
        for temp_folder in self.temp_folders:
            shutil.rmtree(temp_folder, ignore_errors=True)
            log_func("  - Removed %s" % temp_folder)
        self.__temp_folders = []
        log_func("Cleaning done.")


class Runnable(TempFolderProxy):

    status = mongoengine.StringField(required=True, default="pending", choices=('new', 'pending', 'running', 'success', 'error'))
    started = mongoengine.DateTimeField()
    finished = mongoengine.DateTimeField()
    details = mongoengine.StringField(required=False)
    result = mongoengine.DynamicField(required=False)

    def process(self, *args, **kwargs):
        raise NotImplementedError('The "process" method shall be subclassed to define the runnable processing.')

    def pre_process(self, *args, **kwargs):
        # should be overridden
        pass

    def post_process(self, result):
        self.result = result
        return self.result

    def safe_run(self, *args, **kwargs):
        kwargs['safe_run'] = True
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        result = None
        safe_run = kwargs.pop('safe_run', False)
        self.started = datetime.utcnow()
        self.status = 'running'
        self.save()
        try:
            self.log_debug("Launching process...")
            self.pre_process(*args, **kwargs)
            result = self.process(*args, **kwargs)
            result = self.post_process(result)  # strangely can be useful
        except Exception as e:
            self.log_exception(e)
            self.details = "Exception : %s" % str(traceback.format_exc())
            self.status = 'error'
            self.finished = datetime.utcnow()
            self.save()
            if not safe_run:
                raise e
        else:
            self.status = 'success'
            self.finished = datetime.utcnow()
            self.save()
        finally:
            self.log_info("Process done (%s)." % self.status)
            self.clean_temp()
        return result


class AutoDocumentable:
    @classmethod
    def get_doc(cls):
        import textwrap
        def create_documentation(cls):
            result = {
                "class": cls.__name__,
                "doc": textwrap.dedent(cls.__doc__) if cls.__doc__ else '',
            }

            if cls._subclasses:
                subclasses = list(cls._subclasses)
                if cls.__name__ in subclasses:
                    subclasses.remove(cls.__name__)
                if ('Job.' + cls.__name__) in subclasses:
                    subclasses.remove('Job.' + cls.__name__)

                if subclasses:
                    result['sub_classes'] = subclasses

            if cls._meta.get('abstract', False):
                result['abstract'] = True
                return result

            fields = {}
            for key, field in cls._fields.items():
                if key in ['id', '_cls']:
                    continue
                field_dict = {
                    'name': field.name,
                    'required': field.required
                }
                if isinstance(field, mongoengine.EmbeddedDocumentField):
                    field_doc = create_documentation(field.document_type_obj)
                    field_dict['type'] = field_doc['class']
                    if field_doc['doc']:
                        field_dict['type_doc'] = field_doc['doc']
                    if 'fields' in field_doc:
                        field_dict['fields'] = field_doc['fields']
                    if 'abstract' in field_doc:
                        field_dict['abstract'] = field_doc['abstract']
                    if 'sub_classes' in field_doc:
                        field_dict['sub_classes'] = field_doc['sub_classes']
                elif isinstance(field, mongoengine.EmbeddedDocumentListField):
                    field_doc = create_documentation(field.field.document_type_obj)
                    field_dict['type'] = 'List'
                    field_dict['list_type'] = field_doc['class']
                    if 'fields' in field_doc:
                        field_dict['list_type_fields'] = field_doc['fields']
                    if 'abstract' in field_doc:
                        field_dict['abstract'] = field_doc['abstract']
                    if 'sub_classes' in field_doc:
                        field_dict['sub_classes'] = field_doc['sub_classes']
                elif isinstance(field, mongoengine.ListField):
                    field_dict['type'] = 'List'
                    field_dict['list_type'] = field.field.__class__.__name__.replace('Field', '')
                else:
                    field_dict['type'] = field.__class__.__name__.replace('Field', '')

                if hasattr(field, 'doc') and field.doc:
                    field_dict['doc'] = field.doc

                if hasattr(field, 'default') and field.default:
                    if callable(field.default):
                        field_dict['default'] = str(field.default.__name__+'()')
                    else:
                        field_dict['default'] = str(field.default)

                if hasattr(field, 'min_value') and field.min_value:
                    field_dict['min_value'] = field.min_value
                if hasattr(field, 'max_value') and field.max_value:
                    field_dict['max_value'] = field.max_value
                fields[field.name] = field_dict

            result["fields"] = fields
            return result

        return create_documentation(cls)


class Job(NamedDocument, Runnable, LogProxy, AutoDocumentable):

    meta = {
        'collection': 'jobs',
        'indexes': [
            'status',
            'created',
        ]
    }

    status_text = mongoengine.StringField(required=True, default="")
    client_hostname = mongoengine.StringField(required=False)
    client_uuid = mongoengine.StringField(required=False)
    completion = mongoengine.IntField(required=True, min_value=0, max_value=100, default=0)
    timeout = mongoengine.IntField(min_value=0, default=0)
    ttl = mongoengine.IntField(min_value=1, default=1)
    history = mongoengine.ListField(field=mongoengine.DictField(), default=[])

    def __str__(self):
        return "%s (%s)" % (self.name, self.status)

    @cached_property
    def extra_log_arguments(self):
        return {
            'job_type': self.__class__.__name__,
            'job_uuid': self.uuid,
            'job_status': self.status,
            'client_uuid': self.client_uuid,
        }

    #def run(self):
    #    self.update_status(completion=1, text='Running job')
    #    try:
    #        super(Job, self).run()
    #    except Exception as e:
    #        self.save_as_error(text="Error while running job (%s)." % e)
    #        raise e
    #    else:
    #        self.save_as_successful()

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

    #def save_as_successful(self, text='Job Successful'):
    #    self.update_status(completion=100, text=text)

    #def save_as_error(self, text='Job Error'):
    #    self.update_status(text=text)


mongoengine.signals.pre_save.connect(update_modified)


class JobTask(mongoengine.EmbeddedDocument, Runnable, LogProxy, AutoDocumentable):
    meta = {
        'abstract': True,
    }

    @property
    def job(self):
        if isinstance(self._instance, JobTask):
            return self._instance.job
        return self._instance

    @cached_property
    def extra_log_arguments(self):
        extra_log_arguments = {}
        if isinstance(self.job, LogProxy):
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

    #def run(self, *args, **kwargs):
    #    self.status = 'running'
    #    self.save()
    #    result = None
    #    try:
    #        self.log_debug("Launching task process...")
    #        result = super(JobTask, self).run(*args, **kwargs)
    #    except Exception as e:
    #        self.status = 'error'
    #        self.details = "Error while running task (%s)." % e
    #        raise e
    #    else:
    #        self.status = 'success'
    #    finally:
    #        self.save()
    #    return result

    def update_status(self, completion=None, text=None):
        #TODO : Review this part // Completion between tasks and jobs is not clear.
        if text:
            self.job.status_text = text

        if completion:
            self.completion = completion

        log = self.log_info
        if self.status == 'error':
            log = self.log_error

        log("Progress update : {progress:5.1f}% - {message}".format(
            progress=completion if completion else 0,
            message=text
        ))

    def update_progress(self, completion, text=None):
        self.update_status(completion=completion, text=text)

    #def save_as_successful(self, text='Task Successful'):
    #    self.status = 'success'
    #    self.job.update_status(text=text)

    #def save_as_error(self, text='Task Error'):
    #    self.status = 'error'
    #    self.job.update_status(text=text)


class Host(BaseDocument):
    meta = {
        'ordering': ['-updated'],
        'queryset_class': SerializableQuerySet,
        'indexes': [
            'created',
            'updated',
            'hostname'
        ]
    }
    hostname = mongoengine.StringField(required=True)
    mac_address = mongoengine.StringField()
    latest_client = mongoengine.CachedReferenceField(NamedDocument, fields=['uuid'], default=None)
    job_slots = mongoengine.MapField(field=mongoengine.IntField(), default={})
    job_imports = mongoengine.ListField(field=mongoengine.StringField(), default=[])
    platform = mongoengine.DictField()
    boot_time = mongoengine.DateTimeField()
    python_version = mongoengine.StringField()
    python_packages = mongoengine.ListField(field=mongoengine.StringField())


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
    host = mongoengine.CachedReferenceField(Host, fields=['uuid'])
    hostname = mongoengine.StringField(required=True)
    pid = mongoengine.IntField(required=True)
    job_slots = mongoengine.MapField(field=mongoengine.IntField(), default={})
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


