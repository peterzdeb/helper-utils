from sqlalchemy import (Column, Integer, BigInteger, SmallInteger,
                        String, Date, DateTime, Boolean)
from sqlalchemy.ext.declarative import declarative_base


BaseModel = declarative_base()

class SchedulerModel(BaseModel):
    __tablename__ = 'scheduler'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
    lock = Column(String(50))
    retry_cnt = Column(SmallInteger, default=0)
    last_started = Column(DateTime)
    last_finished = Column(DateTime)
    last_status = Column(String(255))


class RunHistoryModel(BaseModel):
    __tablename__ = 'load_history'
    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime)
    finish_time = Column(DateTime)
    exec_status = Column(String(255))
    task_id = Column(Integer)