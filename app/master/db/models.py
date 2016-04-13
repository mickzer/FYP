import logging, time
from master.master_logger import MasterLoggingAdapter
log = logging.getLogger('root_logger')
log = MasterLoggingAdapter(log)

from sqlalchemy import create_engine, asc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, PickleType
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import relationship, backref
from sqlalchemy import and_
from datetime import datetime
import global_conf, uuid, subprocess, os, shutil, json
from aws.s3 import s3
from aws.sqs import new_tasks_queue
from master.job_final_script_executor import JobFinalScriptExecutor

# con_str='mysql://michaeloneill:testing123@fyp-db.caqels6bmmp3.eu-west-1.rds.amazonaws.com:3306/FYP_DB'
con_str = global_conf.DB_CON_STR
engine = create_engine(con_str, pool_size=10)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()
#SQLAlchemy Object JSON Serilaizer
class SerializableBase:
    def to_dict(self, recurse=True, exclude=[]):
        result = {}
        #iterate through col names and relationship names
        for key in self.__mapper__.c.keys() + self.__mapper__.relationships.keys():
            if key in exclude:
                continue
            val = getattr(self, key) #get value of current keys
            if isinstance(val, list):
                if len(val) > 0:
                    if recurse:
                        result[key] = [elem.to_dict(recurse=False) for elem in val]
                else:
                    result[key] = None
            else:
                if isinstance(val, SerializableBase):
                    if recurse:
                        result[key] = val.to_dict(recurse=False)
                else:
                    result[key] = val
        return result

    def to_json(self):
        return json.dumps(self, cls=SQLAlchemyEncoder, indent=2, separators=(',', ': '))

class SQLAlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SerializableBase):
            return obj.to_dict()
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        return json.JSONEncoder.default(self, obj)

class Task(Base, SerializableBase):
    __tablename__='task'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer)
    job_id = Column(String(36), ForeignKey('job.id'), nullable=False)
    file_name = Column(String(256))
    status = Column(String(50), nullable=False, default='created')
    started = Column(DateTime)
    finished = Column(DateTime)
    attributes = Column(PickleType)
    output_data = Column(Boolean)

    def set_session(self, session):
        self.session = session
        self.session.add(self)

    def submit(self):
        """
        Add task to S3 and SQS and change status to submitted
        """
        log.set_job_id(self.job_id)
        log.set_task_id(self.task_id)
        #upload file if relevant
        if self.file_name:
            #s3 key is job-id/file
            s3.put(global_conf.CWD+'job-'+self.job_id+'/split/'+self.file_name, key='job-'+self.job_id+'/split_input/'+self.file_name)
        new_tasks_queue.add_message({'id':self.id, 'attributes':self.attributes, 'job_id':self.job_id, 'task_id':self.task_id, 'file_name':self.file_name})
        try:
            self.status = 'submitted'
            self.session.commit()
            log.info('%s Submitted' % (self))
        except Exception, e:
            log.error('DB Error Submitting Task', exc_info=True)
        log.remove_job_id()
        log.remove_task_id()
    def __repr__(self):
        return '<Task (id=%s, job=%s, task_id=%s, status=%s)>' % (self.id, self.job_id, self.task_id, self.status)

class Job(Base, SerializableBase):
    __tablename__='job'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()), nullable=False)
    name = Column(String(256), nullable=False)
    executable_key_path = Column(String(256), nullable=False)
    input_key_path = Column(String(256))
    data_prep_script = Column(String(256))
    final_script = Column(String(256))
    task_completion_script = Column(String(256))
    created = Column(DateTime, default=datetime.utcnow(), nullable=False)
    finished = Column(DateTime)
    status = Column(String(50), nullable=False, default='created')
    failed_tasks_threshold = Column(Integer, default=0)
    task_split_size = Column(Integer, default=134217728)
    task_completion_context = Column(PickleType, default={})
    tasks = relationship('Task', backref=backref('job'), cascade='delete')
    #will be set on call of submit
    input_dir = None

    def set_session(self, session):
        self.session = session
        self.session.add(self)

    def submit(self):
        log.debug('Submitting %s' % (self))
        self.status = 'submitting'
        self.session.commit()
        log.set_job_id(self.id)
        log.info('Job %s Submitted' % (self))

        log.info('Copying Executable to the S3 Job Folder')
        s3.copy(self.executable_key_path, 'job-'+self.id+'/'+self.id+'.py')

        log.remove_job_id()
        #data is prepared from JobBigOperationController

    def all_tasks_completed(self):
        #check if all tasks are finished
        uncompleted_tasks = self.session.query(Task).filter(and_(Task.job_id == self.id, Task.status != 'completed')).count()
        if uncompleted_tasks > 0:
            failed_tasks = self.session.query(Task).filter(and_(Task.job_id == self.id, Task.status == 'failed')).count()
            return uncompleted_tasks-failed_tasks == 0
        return uncompleted_tasks == 0

    def execute_final_script(self):
        self.status = 'executing final script'
        self.session.commit()
        executor = JobFinalScriptExecutor(self)
        r = executor.run_execution()
        if not r:
            self.mark_as_failed()
        else:
            self.mark_as_completed()

    def mark_as_completed(self):
        log.set_job_id(self.id)
        self.status = 'completed'
        self.finished = datetime.utcnow()
        self.session.commit()
        log.info('Job <%s> Completed' % (self.id))
        log.remove_job_id()
        return True

    def mark_as_tasks_executing(self):
        log.set_job_id(self.id)
        self.status = 'executing tasks'
        self.session.commit()

    def mark_as_failed(self):
        log.set_job_id(self.id)
        self.status = 'failed'
        self.finished = datetime.utcnow()
        self.session.commit()
        #delete task script on s3
        log.info('Deleting Task Script on S3')
        s3.delete('job-'+self.id+'/'+self.id+'.py')
        log.info('Deleted Task Script on S3')
        #delete input splits on s3
        log.info('Deleting Input Splits on S3')
        s3.delete_directory('job-'+self.id+'/split_input/')
        log.info('Deleted Input Splits on S3')
        #mark all unreached tasks as discarded
        discarded_tasks = self.session.query(Task).filter(and_(Task.job_id == self.id, Task.status == 'submitted')).all()
        for task in discarded_tasks:
            task.status = 'discarded'
        self.session.commit()
        log.info('Job <%s> Failed' % (self.id))
        log.remove_job_id()

    def is_running(self):
        if self.status == 'completed' or self.status == 'failed':
            return False
        return True

    def output_log_to_s3(self):
        log.info('Pulling all log data from DB for %s' % (self))
        filename = global_conf.CWD+self.id+'.log'
        f = open(filename, 'w+')
        for l in self.session.query(Log).filter(Log.job_id == self.id).order_by(asc(Log.date)).yield_per(1000):
            f.write(str(l)+'\n')
        f.close()
        log.info('Log Generated. Uploading to S3')
        r = s3.put(filename, key='/job-'+self.id+'/'+self.id+'.log')
        os.remove(filename)
        return r

    def update_task_completion_context(self, context):
        self.task_completion_context = context
        self.session.add(self)
        self.session.commit()

    def __repr__(self):
        return '<Job (id=%s, status=%s)>' % (self.id, self.status)

class Log(Base, SerializableBase):
    __tablename__='log'

    id = Column(Integer, primary_key=True)
    type = Column(String(6), nullable=False)
    job_id = Column(String(36), ForeignKey('job.id'))
    task_id = Column(Integer)
    level = Column(String(10), nullable=False)
    instance_id = Column(String(12), nullable=False)
    pathname = Column(String(150), nullable=False)
    msg = Column(String(500), nullable=False)
    date = Column(DATETIME(fsp=6), nullable=False)
    task_message = Column(Boolean, nullable=False, default=False)

    def __init__(self, **kwargs):
        Base.__init__(self)
        #check if msg is a dict, if json it
        if isinstance(kwargs['msg'], dict):
            kwargs['msg'] = json.dumps(kwargs['msg'])
        elif not kwargs['msg']:
            kwargs['msg'] = 'NULL'
        for key in kwargs:
            try:
                setattr(self, key, kwargs[key])
            except:
                pass

    def __repr__(self):
        #gives a representation similar to the usual python logging format
        msg = '%s: %s - %s - %s - %s' % (self.type.upper(), self.instance_id, self.date, self.level, os.path.relpath(self.pathname))
        if self.task_message:
            msg += ' - TASK_FUNCTION_MSG'
        return msg + ' - %s' % (self.msg)
