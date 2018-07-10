#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 nu
"""
(c) 2015 Ronan Delacroix
Job Manager Common sub module
:author: Ronan Delacroix
"""
import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from datetime import datetime
import os
import logging
import tempfile
import shutil
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


class ConfigurationException(Exception):
    pass


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


def change_keys(obj, convert):
    """
    Recursively goes through the dictionary obj and replaces keys with the convert function.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in obj.items():
            new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


def replace_type_cls(key):
    if key == 'type':
        return '_cls'
    return key


def replace_cls_type(key):
    if key == '_cls':
        return 'type'
    return key


class SerializableQuerySet(mongoengine.QuerySet):

    def to_json(self):
        return tbx.text.render_json(self.as_pymongo())

    def to_safe_dict(self):
        return [f.to_safe_dict() for f in self]
        #return [public_dict(f) for f in self.as_pymongo()]


def safely_import_from_name(modules):
    if modules:
        logging.info('Starting initial module import')
        logging.info('Trying to import the following : ' + ', '.join(modules))
    else:
        logging.warning('No modules to import.')
    for mod in modules:
        try:
            globals()[mod] = __import__(mod)
        except ImportError as e:
            logging.error("Can't import Job module '%s' as defined in settings. Exiting." % mod)
            raise ConfigurationException(e)
    if modules:
        logging.info('Modules import OK.')


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
            self.name = self.type + ' ' + self.uuid

    def __repr__(self):
        return self.name


class LogProxy(object):

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


class TempFolderProxy(object):

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
            self.save_as_error()
            if not safe_run:
                raise e
        else:
            self.status = 'success'
            self.finished = datetime.utcnow()
            self.save_as_successful()
        finally:
            self.log_info("Process ended (%s)." % self.status)
            self.clean_temp()
        return result

    def save_as_successful(self):
        self.save()

    def save_as_error(self):
        self.save()


class AutoDocumentable(object):
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
                if hasattr(field, 'choices') and field.choices:
                    field_dict['choices'] = list(field.choices)
                fields[field.name] = field_dict

            result["fields"] = fields
            return result

        return create_documentation(cls)

