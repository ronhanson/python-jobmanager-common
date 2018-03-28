#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 nu
"""
:Author: Ronan Delacroix
:Copyright: (c) 2018 Ronan Delacroix
"""
import mongoengine
import jobmanager.common


class DockerImage(jobmanager.common.NamedDocument):
    meta = {
        'queryset_class': jobmanager.common.SerializableQuerySet,
        'indexes': [
            'uuid',
            'created',
            'name'
        ]
    }
    name = mongoengine.StringField(required=True)
    image_id = mongoengine.StringField()
    url = mongoengine.ListField(mongoengine.StringField())
    tags = mongoengine.ListField(field=mongoengine.StringField())
    jobs = mongoengine.ListField(field=mongoengine.StringField())
    tasks = mongoengine.ListField(field=mongoengine.StringField())
    requirements = mongoengine.ListField(field=mongoengine.StringField())
    apt_packages = mongoengine.ListField(field=mongoengine.StringField())
    dockerfile = mongoengine.StringField()
