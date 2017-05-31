#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 nu
"""
(c) 2017 Ronan Delacroix
Job Manager Job Examples
:author: Ronan Delacroix
"""
import logging
import tbx.process
import mongoengine
from . import job


class ExecuteJob(job.Job):
    command = mongoengine.StringField(required=True)
    output = mongoengine.StringField(default=None)

    def process(self):
        self.log_info('ExecuteJob %s - Executing command...' % self.uuid)
        result = tbx.process.execute(self.command, return_output=True, logger=logging.getLogger())
        self.log_info(result)
        self.output = result


class WaitJob(job.Job):
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