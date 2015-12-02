import logging
log = logging.getLogger('root_logger')

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import relationship, backref
from sqlalchemy import and_
from datetime import datetime
import global_conf, uuid, subprocess, os, shutil, json
from aws.s3 import s3
from aws.sqs import new_tasks_queue
from master.job_final_script_executor import JobFinalScriptExecutor

# con_str='mysql://michaeloneill:testing123@fyp-db.caqels6bmmp3.eu-west-1.rds.amazonaws.com:3306/FYP_DB'
con_str='mysql+pymysql://michaeloneill:testing123@fyp-db.caqels6bmmp3.eu-west-1.rds.amazonaws.com:3306/FYP_DB'
engine = create_engine(con_str, pool_size=10)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()
#SQLAlchemy Object JSON Serilaizer
class SerializableBase:
    def to_dict(self, recurse=True):
        result = {}
        #iterate through col names and relationship names
        for key in self.__mapper__.c.keys() + self.__mapper__.relationships.keys():
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
    file_name = Column(String(256), nullable=False)
    status = Column(String(50), nullable=False, default='created')
    started = Column(DateTime)
    finished = Column(DateTime)

    def set_session(self, session):
        self.session = session
        self.session.add(self)

    def submit(self):
        """
        Add task to S3 and SQS and change status to submitted
        """
        #s3 key is job-id/file
        s3.put(global_conf.CWD+'job-'+self.job_id+'/input/'+self.file_name, key='job-'+self.job_id+'/split_input/'+self.file_name)
        new_tasks_queue.add_message({'id':self.id, 'job_id':self.job_id, 'task_id':self.task_id, 'file_name':self.file_name})
        try:
            self.status = 'submitted'
            self.session.commit()
        except Exception, e:
            log.error('DB Error Submitting Task', exc_info=True)
        log.info('%s Submitted' % (self))

    def __repr__(self):
        return '<Task (id=%s, job=%s, status=%s)>' % (self.id, self.job_id, self.status)

class Job(Base, SerializableBase):
    __tablename__='job'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()), nullable=False)
    name = Column(String(256), nullable=False)
    executable_key_path = Column(String(256), nullable=False)
    input_key_path = Column(String(256), nullable=False)
    final_script = Column(String(256))
    created = Column(DateTime, default=datetime.utcnow(), nullable=False)
    finished = Column(DateTime)
    status = Column(String(50), nullable=False, default='created')
    failed_tasks_threshold = Column(Integer, default=0)
    task_split_size = Column(Integer, default=134217728)
    tasks = relationship('Task', backref=backref('job'), cascade='delete')
    #will be set on call of submit
    input_dir = None

    def set_session(self, session):
        self.session = session
        self.session.add(self)

    def submit(self):
        log.info('Submitting %s' % (self))
        self.status = 'submitting'
        self.session.commit()

        log.info('Copying Executable to the S3 Job Folder')
        s3.copy(self.executable_key_path, 'job-'+self.id+'/'+self.id+'.py')
        log.info('Creating Tasks for Job: %s' % (self))
        #set the input dir, don't do this until now as a
        #job must be commited before it has an id
        self.input_dir = global_conf.CWD+'job-'+self.id+'/input/'
        #download and split input data
        self._split_input_data()
        tasks = []
        #create a task for each split
        try:
            for f in os.listdir(self.input_dir):
                t=Task(job_id=self.id, file_name=f, task_id=f[f.rfind('_')+1:])
                #-1 means the last element
                self.session.add(t)
                self.session.commit()
                log.info('Created %s' % (t))
                #give task the session to work with later
                t.set_session(self.session)
                tasks.append(t)

            for task in tasks:
                task.submit()

            #delete input folder after tasks ha ve been created and
            #splits saved to S3
            shutil.rmtree(self.input_dir)

            #job is submitted so mark as executing
            self.status = 'executing tasks'
            self.session.commit()

        except Exception, e:
            log.error('Error Creating Task', exc_info=True)

    def _split_input_data(self):
        log.info('Downloading & Splitting Input Data')
        #-------
        def split_file(file_path):
            """Does a bash command to split a file_path
            using the bash split command as it's probably more efficient
            than anything python can do!
            Only problem is that it makes windows a no go
            """
            log.info('Splitting file %s' % (file_path))
            try:
                #rename file to put _ at the end
                #turns /input/asd.txt -> /input/asd_.txt
                file_name = os.path.splitext(os.path.basename(file_path))
                new_file_path = file_path[:file_path.rfind('/')+1]+file_name[0]+'_'+file_name[1]
                os.rename(file_path, new_file_path)
                #split file with block size using numerical indexes with the file prefix as split prefixes ie. file_name.txt -> file_name0, file_name1...
                cmd = "split %s -b %s -d %s" % (os.path.basename(new_file_path), self.task_split_size, os.path.splitext(os.path.basename(new_file_path))[0])
                process = subprocess.Popen(cmd.split(), cwd=self.input_dir, stdout=subprocess.PIPE)
                #wait until the split finishes
                process.wait()
                #delete the original file afterwards
                os.remove(new_file_path)
            except Exception, e:
                log.error('Error Splitting Input:', exc_info=True)
        #-------
        #download input directory from s3 into a folder input
        r=s3.get_directory(self.input_key_path, self.input_dir)
        log.info('Input files downloaded from S3')
        #go through each file in the directory
        for f in os.listdir(self.input_dir):
            split_file(self.input_dir+f)
        log.info('Finished Splitting Input')

    def all_tasks_completed(self):
        #check if all tasks are finished
        return self.session.query(Task).filter(and_(Task.job_id == self.id, Task.status != 'completed')).count()  == 0

    def execute_final_script(self):
        self.status = 'executing final script'
        self.session.commit()
        executor = JobFinalScriptExecutor(self)
        if not executor.run_execution():
            self.mark_as_failed()
        else:
            self.mark_as_completed()

    def mark_as_completed(self):
        self.status = 'completed'
        self.finished = datetime.utcnow()
        self.session.commit()
        log.info('Job <%s> Completed' % (self.id))
        return True

    def mark_as_failed(self):
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

    def is_running(self):
        if self.status == 'completed' or self.status == 'failed':
            return False
        return True

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
        for key in kwargs:
            try:
                setattr(self, key, kwargs[key])
            except:
                pass

    def __repr__(self):
        #gives a representation similar to the usual python logging format
        msg = '%s - %s - %s - %s' % (self.date, self.level, os.path.relpath(self.pathname), self.instance_id)
        if self.task_message:
            msg += ' - TASK_FUNCTION_MSG'
        return msg + ' - %s' % (self.msg)
