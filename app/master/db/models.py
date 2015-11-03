import logging
log = logging.getLogger('root_logger')

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref
from datetime import datetime
import global_conf, uuid, subprocess, os, shutil
from aws.s3 import s3
from aws.sqs import new_tasks_queue

con_str='mysql+pymysql://michaeloneill:testing123@fyp-db.caqels6bmmp3.eu-west-1.rds.amazonaws.com:3306/FYP_DB'
engine = create_engine(con_str)

Session = sessionmaker()

Base = declarative_base()
Session.configure(bind=engine)
session = Session()

class Task(Base):
    __tablename__='task'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer)
    job_id = Column(String(36), ForeignKey('job.id'), nullable=False)
    file_name = Column(String(256), nullable=False)
    status = Column(String(50), nullable=False, default='created')
    started = Column(DateTime, default=datetime.utcnow())
    finished = Column(DateTime)

    def submit(self):
        """
        Add task to S3 and SQS and change status to submitted
        """
        #s3 key is job-id/file
        s3.put(global_conf.CWD+'input/'+self.file_name, key='job-'+self.job_id+'/split_input/'+self.file_name)
        new_tasks_queue.add_message({'id':self.id, 'job_id':self.job_id, 'task_id':self.task_id, 'file_name':self.file_name})
        try:
            self.status = 'submitted'
            session.commit()
        except Exception, e:
            log.error('DB Error Submitting Task', exc_info=True)

    def __repr__(self):
        return '<Task (id=%s, status=%s)>' % (self.id, self.status)

class Job(Base):
    __tablename__='job'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()), nullable=False)
    name = Column(String(256), nullable=False)
    executable_key_path = Column(String(256), nullable=False)
    input_key_path = Column(String(256), nullable=False)
    export_key_path = Column(String(256), nullable=False)
    master_key_path = Column(String(256), nullable=False)
    created = Column(DateTime, default=datetime.utcnow(), nullable=False)
    finished = Column(DateTime)
    status = Column(String(50), nullable=False, default='created')
    tasks = relationship('Task', backref=backref('job'), cascade='delete')
    input_dir = global_conf.CWD+'input/'

    def create_tasks(self):
        #download and split input data
        self._split_input_data()
        tasks = []
        #create a task for each split
        try:
            for f in os.listdir(self.input_dir):
                tasks.append(Task(job_id=self.id, file_name=f, task_id=f[f.rfind('_')+1:]))
                session.add(tasks[-1])
            session.commit()

            for task in tasks:
                task.submit()

            #delete input folder after tasks ha ve been created and
            #splits saved to S3
            shutil.rmtree(self.input_dir)
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
                cmd = "split %s -b %s -d %s" % (os.path.basename(new_file_path), global_conf.SPLIT_SIZE, os.path.splitext(os.path.basename(new_file_path))[0])
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

    def __repr__(self):
        return '<Job (id=%s, status=%s)>' % (self.id, self.status)
