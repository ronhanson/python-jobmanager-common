#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 nu
"""
(c) 2015-2017 Ronan Delacroix
Job Manager Host
:author: Ronan Delacroix
"""
import os
import sys
import logging
import socket
import tbx.code
from datetime import datetime, timedelta
import platform
import socket
import psutil
import pkg_resources
import mongoengine
import jobmanager
import jobmanager.common as common


class Host(common.BaseDocument):
    meta = {
        'ordering': ['-updated'],
        'queryset_class': common.SerializableQuerySet,
        'indexes': [
            'created',
            'updated',
            'hostname'
        ]
    }
    hostname = mongoengine.StringField(required=True)
    pid = mongoengine.IntField(required=True)
    mac_address = mongoengine.StringField()
    job_slots = mongoengine.MapField(field=mongoengine.IntField(), default={})
    job_imports = mongoengine.ListField(field=mongoengine.StringField(), default=[])
    platform = mongoengine.DictField()
    boot_time = mongoengine.DateTimeField()
    python_version = mongoengine.StringField()
    python_packages = mongoengine.ListField(field=mongoengine.StringField())

    def history(self, offset=0, limit=30, step=0):
        step_filter = {}
        if step and step > 1:
            step_filter = {'index__mod':(step,0)}
        statuses = HostStatus.objects(host=self, **step_filter).order_by('-created')[offset:limit]
        return [s.to_safe_dict(with_host=False) for s in statuses]

    def alive(self):
        recent_count = HostStatus.objects(host=self, created__gte=datetime.utcnow() - timedelta(minutes=0.5)).count()
        return recent_count > 0

    def last_seen_alive(self):
        last_status = HostStatus.objects(host=self).order_by('-created').only('created').first()
        if not last_status or not last_status.created:
            return None
        return last_status.created

    def to_safe_dict(self, alive=False, with_history=False, offset=0, limit=30, step=0):
        r = super(Host, self).to_safe_dict()
        if alive:
            r['alive'] = self.alive()
            r['last_seen_alive'] = self.last_seen_alive()
        if with_history:
            r['history'] = self.history(offset=offset, limit=limit, step=step)
        return r

    def update_status(self):
        partitions = []
        try:
            for f in psutil.disk_partitions():
                usage = psutil.disk_usage(path=f.mountpoint)
                p = {
                    'type': f.fstype,
                    'device': f.device,
                    'mountpoint': f.mountpoint,
                    'total': usage.total,
                    'used': usage.used,
                    'percent': usage.percent,
                }
                partitions.append(p)
        except Exception as e:
            pass

        self_process = psutil.Process(os.getpid())
        processes = [{'ppid': self_process.ppid(), 'pid': self_process.pid, 'cmd': ' '.join(self_process.cmdline())}]
        for c in self_process.children():
            try:
                processes.append({'ppid': c.ppid(), 'pid': c.pid, 'cmd': ' '.join(c.cmdline())})
            except psutil.Error:
                pass

        self.host_status_index += 1

        status = HostStatus()
        status.index = self.host_status_index
        status.host = self
        status.current_jobs = [{'uuid': j.uuid, 'type': j._cls} for j in self.client_service.current_jobs]

        virtual_memory = psutil.virtual_memory()
        swap_memory = psutil.swap_memory()

        status.system_status = {
            'processes': processes,
            'cpu': {
                'percent': psutil.cpu_percent(),
                'percents': psutil.cpu_percent(percpu=True)
            },
            'memory': {
                'virtual': {
                    'total': virtual_memory.total,
                    'used': virtual_memory.used,
                    'percent': virtual_memory.percent,
                },
                'swap': {
                    'total': swap_memory.total,
                    'used': swap_memory.used,
                    'percent': swap_memory.percent,
                },
            },
            'disk': partitions,
            #'disk_io': safe_dict(psutil.disk_io_counters, perdisk=False)
        }
        status.save()

    @classmethod
    def localhost(cls):
        hostname = socket.gethostname()
        hosts = Host.objects(hostname=hostname)
        if not hosts:
            logging.info('Host unknown. Initializing it in the database...')
            host = Host()
            host.hostname = hostname
            host.mac_address = tbx.network.get_mac_address()
            host.platform = tbx.code.safe_dict(platform.uname)
            host.host_status_index = 1
            logging.info("Now, configure Host '%s' through API or Web UI to be able to use it." % hostname)
        else:
            host = hosts[0]
            last_status = HostStatus.objects(host=host).order_by('-created').first()
            if not last_status:
                host.host_status_index = 1
            else:
                host.host_status_index = last_status.index
            logging.info("Host '%s' already found in database." % hostname)

        host.boot_time = datetime.fromtimestamp(psutil.boot_time())
        host.pid = os.getpid()
        host.python_version = sys.version.split(' ')[0]
        host.python_packages = sorted(["%s (%s)" % (i.key, i.version) for i in pkg_resources.working_set])
        host.save()
        logging.info("Host '%s' config updated in database." % hostname)
        return host

    def update_slots(self, job_slots=None):
        from jobmanager.common.job import Job, JobTask
        job_classes = tbx.code.get_subclasses(Job)
        job_tasks = tbx.code.get_subclasses(JobTask)
        if not job_slots:
            logging.info('Job Slots not set in env or command line args. Setting to default job defined amount.')
            job_slots = {k.__name__: k.default_slot_amount() for k in job_classes}
        available_class_names = {c.__name__ for c in job_classes}
        previous_class_names = set(self.job_slots.keys())
        all_class_names = previous_class_names | available_class_names
        for class_name in all_class_names:
            if class_name not in job_slots.keys():
                self.job_slots[class_name] = 0
            else:
                self.job_slots[class_name] = job_slots[class_name]
            logging.info(" - Job type found : %s (%d slots)" % (class_name, self.job_slots[class_name]))
        logging.info("Also found following job tasks : %s" % ', '.join([k.__name__ for k in job_tasks]))
        return self.save()

    def check_capacity(self):
        if self.job_slots:
            #logging.info("Jobs types allowed : %s" % (', '.join(self.job_slots.keys())))
            logging.info("Jobs capacity :")
            total_capacity = 0
            for c in self.job_slots:
                logging.info(" - %s\t: %d" % (c, self.job_slots[c]))
                total_capacity += self.job_slots[c]

            if total_capacity == 0:
                logging.error("No job capacities setup. Configure host to add slots to some job types.")
                raise common.ConfigurationException(
                    "No job capacities setup. Configure host to add slots to some job types (see --add-slot option).")
        else:
            logging.error(
                "No job class found to be run. Configure host to import packages that contain job subclasses.")
            raise common.ConfigurationException(
                "No Job sub-class found in imports. Please configure host (see --import option).")

    def do_import(self, imports):
        return common.safely_import_from_name(imports)

    @classmethod
    def get_all_alive(cls):
        raise NotImplementedError()


class HostStatus(common.BaseDocument):

    meta = {
        'ordering': ['-created'],
        'max_documents': 200000,
        'max_size': 200000000,
        'queryset_class': common.SerializableQuerySet,
        'indexes': [
            'created',
            'host'
        ]
    }
    host = mongoengine.CachedReferenceField(Host, fields=['hostname'], reverse_delete_rule=mongoengine.CASCADE)
    index = mongoengine.LongField(required=True, default=0)
    system_status = mongoengine.DictField(default={})
    current_jobs = mongoengine.ListField(field=mongoengine.DictField(), default=[])
    updated = None

    def to_safe_dict(self, with_host=True):
        r = super(HostStatus, self).to_safe_dict()
        if not with_host:
            del r['host']
            del r['type']
        return r
