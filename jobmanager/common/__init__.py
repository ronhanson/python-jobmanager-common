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
