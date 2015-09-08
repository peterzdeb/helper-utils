import logging
from optparse import OptionParser
import sys

import sqlalchemy

from scheduler_config import *
from models import BaseModel
from scheduler import AppScheduler


if __name__ == '__main__':
    opt = OptionParser(usage='usage: %prog [options] app_name')

    opt.add_option("-l", "--log_level", dest="log_level", default='INFO',
                      help="verbosity log level (default INFO)")
    opt.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="force running without scheduling")
    opt.add_option("-i", "--init",
                      action="store_true", dest="init", default=True,
                      help="initialize scheduler storage")


    (options, args) = opt.parse_args()

    if not args:
        sys.exit('Application is not specified!')
    else:
        app_name = args[0]

    log_level = options.log_level.upper()
    if not log_level in ['DEBUG', 'INFO', 'WARNING', 'WARN', 'ERROR', 'CRITICAL']:
        sys.exit('Unknown log level specified: %s' % log_level)

    logging.basicConfig()
    logger = logging.getLogger('app-runner')
    logger.setLevel(log_level)

    logger.debug('App scheduler started with flags: %s', str(options))
    try:
        logger.debug('Initializing database engine')
        engine = sqlalchemy.create_engine(
            '%(dialect)s://%(user)s:%(password)s@%(host)s/%(database)s' % APP_SCHEDULER_DB,
            max_overflow=0)
        session = sqlalchemy.orm.sessionmaker(bind=engine)()

        if options.init:
            BaseModel.metadata.create_all(bind=engine)
        logger.debug('Connection to MetaDB established')
    except (sqlalchemy.exc.DBAPIError, sqlalchemy.exc.SQLAlchemyError) as e:
        logger.exception('Cannot connect to DB: %s' % e)
        sys.exit(1)

    scheduler = AppScheduler(app_name, APP_SCHEDULE, session)
    logger.info('Initialized scheduler for application %s', app_name)
    try:
        if options.force or scheduler.should_start():
            logger.info('Ready to start application: %s', app_name)
            error = ''
            try:
                try:
                    if scheduler.lock():
                        scheduler.run()
                except Exception as e:
                    error = str(e)
                    raise
            finally:
                scheduler.unlock(error)
    except Exception as e:
        logger.exception('Runtime error in scheduler: %s' % e)
        sys.exit(1)
    logger.info('App Scheduler completed all planned tasks')
    sys.exit(0)
