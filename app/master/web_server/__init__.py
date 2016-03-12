from flask import Flask, request, render_template

class WebServer:
    def __init__(self, job_big_operation_controller):
        self.job_big_operation_controller = job_big_operation_controller

    def start(self, debug=False):
        try:
            data['job_big_operation_controller'] = self.job_big_operation_controller
            if debug:
                master_app.run(host='0.0.0.0', debug=True)
            else:
                master_app.run(host='0.0.0.0')
        except Exception,e :
            print str(e)


def create_master_app():
    factory_master_app = Flask(__name__)
    return factory_master_app

master_app = create_master_app()
#importable dict for shared stuff
#can't get flask to work with anything but a dict
data = {}
#set mimetype to json for each response
@master_app.after_request
def after_request(response):
    if '/static/' not in request.path and request.endpoint != 'index':
            response.mimetype = 'application/json'
    return response


@master_app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

from master.web_server.rest_api import job, job_log, task_log
