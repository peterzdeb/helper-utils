from datetime import datetime
import importlib
import logging
import socket
import time

from sqlalchemy.exc import IntegrityError

from scheduler_config import *
from models import SchedulerModel, RunHistoryModel
from utils import get_datetime_struct


class AppScheduler(object):
    """
    This class takes application module name (from 'applications' package) and
    run it when scheduling time passes to the app configuration.

    Each Application module must have function "run()" which is called by the AppScheduler
    """

    def __init__(self, app_name, sched_conf, session):
        self._log = logging.getLogger('scheduler')
        self.app_name = app_name
        try:
            app_conf = sched_conf[self.app_name]
        except KeyError:
            raise Exception('No scheduling settings for application: %s' % self.app_name)
        self.enabled = app_conf.get('enabled', False)
        self.months = app_conf.get('months', '*')
        self.month_week = app_conf.get('month_week', '*')
        self.days = app_conf.get('days', '*')
        self.week_days = app_conf.get('week_days', '*')
        self.hours = app_conf.get('hours', '*')
        self.minutes = app_conf.get('minutes', '*')
        self.frequency = app_conf.get('frequency', None)
        if not any([self.months, self.month_week, self.days, self.week_days,
                    self.hours, self.minutes, self.frequency]):
            raise Exception('Scheduling configuration for "%s"" is empty. Please set "frequency" or any timing value',
                            self.app_name)
        self.retry_limit = app_conf.get('retry_limit', 3)
        self.retry_delay = app_conf.get('retry_delay', 60)
        self.session = session
        self.__last_started = None

    def __check_timestamp(self, timestamp):
        def __match_field(field, value):
            result = None
            if field == '*':
                result = field
            elif isinstance(field, list):
                for item in field:
                    if item == value:
                        result = value
                        break
            else:
                if field == value:
                    result = value
            return result

        year, month, mweek, mday, last_mweek, last_mday, wday, hour, min = \
                                            get_datetime_struct(timestamp)
        __month_week = self.month_week
        if __month_week != '*':
            if __month_week > last_mweek:
                __month_week = last_mweek

        __month_days = self.days
        if __month_days != '*':
            __new_mdays = []
            for d in __month_days:
                if last_mday > d:
                    d = last_mday
                __new_mdays.append(d)
            __month_days = __new_mdays

        matches = [__match_field(arg, value) for arg, value in (
                        [(self.months, month),
                         (__month_week, mweek),
                         (__month_days, mday),
                         (self.week_days, wday),
                         (self.hours, hour),
                         (self.minutes, min)])]
        result = None
        checksum_parts = []
        val_found = False
        for val, default in zip(matches, [month, mweek, mday, wday, hour, min]):
            if val == '*' and not val_found:
                checksum_parts.append(default)
            else:
                checksum_parts.append(val)
                val_found = True
        if not any([1 for i in checksum_parts if i is None]):
            return ''.join([str(p) for p in checksum_parts])

    def __expire_loads(self):
        app_objs = self.session.query(SchedulerModel).filter(
                        SchedulerModel.lock.isnot(None),
                        SchedulerModel.last_started < datetime.now() - timedelta(seconds=APP_EXPIRE_TIMEOUT))
        if app_objs.count() > 0:
            self._log.info('Found %d locked out application that already need to'
                           'be expired (%d secs since start)',
                           app_objs.count(),
                           APP_EXPIRE_TIMEOUT)
            for app in app_objs.all():
                app.lock = None
            self.session.commit()

    def _add_to_scheduler(self):
        """Add application to the scheduler."""
        try:
            self._log.info("Initializing application scheduling: '%s'", self.app_name)
            app_obj = SchedulerModel(name=self.app_name)
            self.session.add(app_obj)
            self.session.commit()
        except IntegrityError:
            # Application with given name already scheduled
            # rollback transaction
            self.session.rollback()

    def should_start(self):
        self.__expire_loads()

        app_objs = self.session.query(SchedulerModel).filter_by(name=self.app_name)
        current_timestamp = time.time()
        result = False
        if app_objs.count() > 0:
            app = app_objs.one()
            if app.lock:
                result = False
            elif not app.last_started:
                result = True
            elif self.frequency:
                result = bool((datetime.today() - app.last_started).seconds >
                              int(self.frequency))
            elif app.last_status and (self.retry_limit == 0 or app.retry_cnt < self.retry_limit) and \
                (datetime.today() - app.last_started).seconds >= self.retry_delay:
                result = True
            else:
                current_time_checksum = self.__check_timestamp(current_timestamp)
                last_started = time.mktime(app.last_started.timetuple())
                result = ( current_time_checksum and
                             ( not last_started or
                               current_time_checksum != self.__check_timestamp(last_started)))
        else:
            self._add_to_scheduler()
            result = True
        return result

    def lock(self):
        locked = False
        current_status = "RUNNING on '%s'" % socket.gethostname()
        query = self.session.query(SchedulerModel).filter_by(name=self.app_name)
        app_obj = query.with_lockmode('update').one()
        if not app_obj.lock:
            self._log.info("Locking application '%s'", self.app_name)
            app_obj.lock = current_status
            self.__last_started = datetime.now()
            app_obj.last_started = self.__last_started
            locked = True
        else:
            self.__last_started = app_obj.last_started
            self._log.warning("Application '%s' is already locked in parallel process", self.app_name)
        self.session.commit()
        return locked

    def unlock(self, error):
        if not self.__last_started:
            return
        error = error.strip()
        finish_time = datetime.now()
        self._log.info("Unlocking application '%s'", self.app_name)
        app_objs = self.session.query(SchedulerModel).filter_by(
                                                name=self.app_name)
        if app_objs.count() == 0:
            app = SchedulerModel(name=self.app_name,
                                 lock=None,
                                 last_started=self.__last_started,
                                 last_finished=finish_time,
                                 last_status=error)
            self.session.add(app)
        else:
            app = app_objs.one()
            app.last_finished = finish_time
            if not error:
                app.retry_cnt = 0
                app.last_status = None
            else:
                app.retry_cnt += 1
                app.last_status = error
            app.lock = None

        self._log.info("Logging application '%s' run to history. Exec time is %.2f secs" %
                       (self.app_name,
                        float((finish_time - self.__last_started).microseconds)/10**6))
        history = RunHistoryModel(task_id=app.id,
                              start_time=self.__last_started,
                              finish_time=finish_time,
                              exec_status=error)
        self.session.add(history)
        self.session.commit()
        self.__last_started = None

    def run(self):
        module_name = '.'.join(['scheduler', 'applications', self.app_name])
        app_module = importlib.import_module(module_name)
        self._log.info('Running application %s', self.app_name)
        app_module.run()
