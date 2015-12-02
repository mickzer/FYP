from flask import request
from master.web_server import master_app, data
from master.web_server.response_helpers import *
from master.db.models import Session, Job
import sqlalchemy.exc

@master_app.route('/api/job/', methods=['GET', 'POST'])
@master_app.route('/api/job/<string:job_id>/', methods=['GET'])
def job(job_id=None):
    if request.method == 'GET':
        session = Session()
        if job_id:
            job = session.query(Job).filter(Job.id == job_id).first()
            return job.to_json() if job else (not_found(), 404)
        else:
            jobs = session.query(Job).all()
            return json_out(jobs) if jobs else (not_found(), 404)
    elif request.method == 'POST':
        session = Session()
        # try:
        j = Job(**request.get_json())
        session.add(j)
        session.commit()
        job=j.to_json()
        session.close()
        #add operation to queue
        data['job_big_operation_controller'].add(j)
        return job
        # except sqlalchemy.exc.IntegrityError:
        #     return bad_request(), 400
        # except:
        #     return internal_error(), 500

@master_app.route('/api/job/<string:job_id>/tasks/', methods=['GET'])
def task(job_id=None):
    session = Session()
    tasks = session.query(Task).filter(Task.job_id == job_id).all()
    if tasks:
        tasks = json_out(tasks)
        session.close()
        return tasks
    else:
        session.close()
        return not_found(), 404
