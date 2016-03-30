from flask import request
from master.web_server import master_app, data
from master.web_server.response_helpers import *
from master.db.models import Session, Job, Task, Log
from sqlalchemy import and_
import sqlalchemy.exc

@master_app.route('/api/job/', methods=['GET', 'POST'])
@master_app.route('/api/job/<string:job_id>/', methods=['GET'])
def job(job_id=None):
    if request.method == 'GET':
        session = Session()
        if job_id:
            job = session.query(Job).filter(Job.id == job_id).first()
            r = job.to_json() if job else (not_found(), 404)
            session.close()
            return r
        else:
            jobs = session.query(Job).all()
            r = json_out(jobs, exclude=['tasks']) if jobs else (not_found(), 404)
            session.close()
            return r
    elif request.method == 'POST':
        session = Session()
        try:
            j = Job(**request.get_json())
            session.add(j)
            session.commit()
            job=j.to_json()
            session.close()
            #add operation to queue
            data['job_big_operation_controller'].add(j)
            return job
        except sqlalchemy.exc.IntegrityError:
            return bad_request(), 400
        except:
            return internal_error(), 500

@master_app.route('/api/job/<string:job_id>/task/<int:task_id>/log/', methods=['GET'])
def task_log(job_id=None, task_id=-1):
    session = Session()
    logs = session.query(Log).filter(and_(Log.job_id == job_id, Log.task_id == task_id)).all()
    r = json_out(logs) if logs else (not_found(), 404)
    session.close()
    return r

@master_app.route('/api/job/<string:job_id>/log_to_s3/', methods=['GET'])
def job_log(job_id=None):
    session = Session()
    job = session.query(Job).filter(Job.id == job_id).first()
    if job:
        job.output_log = True
        session.close()
        #add operation to queue
        data['job_big_operation_controller'].add(job)
        #doing JSON response to keep my JS simpler
        return json.dumps({'success': True})
    else:
        session.close()
        return ('', 404)
