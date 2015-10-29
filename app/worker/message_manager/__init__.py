import threading, time
from aws.sqs import new_tasks_queue

class MessageManager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        #keeps telling sqs not to make the task message
        #visible until the task is completed
        while True:
            #causes the thread to stop when the
            #task message has been deleted
            if not new_tasks_queue.retain_message():
                break
            time.sleep(20)
