import logger
log = logger.create_logger('root_logger')
log.debug('Logger Created')

import global_conf, click

@click.group()
def cli():
    pass

@cli.command()
def assume_worker():
    from worker import Worker
    log.info('Assuming Worker Role')
    w=Worker()
    w.run()

@cli.command()
def provision_workers():
    from master.worker_manager import launch_workers
    launch_workers()

@cli.command()
@click.option('--name', help='Name of Job')
@click.option('--executable_key_path', help='S3 key path of executable')
@click.option('--input_key_path', help='S3 key path of input data directory')
@click.option('--final_script_key_path', help='S3 key path of final script')
def provision_job(name, executable_key_path, input_key_path, final_script_key_path):
     from master.job_manager import  create_job, submit_job
     job_id = create_job(name=name, executable_key_path=executable_key_path, input_key_path=input_key_path, export_key_path='/none', final_script_key_path=final_script_key_path)
     submit_job(job_id)
     from master.receiver import Receiver
     log.info('Job Submitted, Starting Receiver')
     r = Receiver()
     r.receive()

@cli.command()
def receive():
    from master.receiver import Receiver
    log.info('Starting Receiver')
    r = Receiver()
    r.receive()

@cli.command()
def delete_worker_messages():
    from aws.sqs import workers_messaging_queue
    workers_messaging_queue.delete_all_messages()

@cli.command()
def delete_task_messages():
    from aws.sqs import new_tasks_queue
    new_tasks_queue.delete_all_messages()

@cli.command()
def test():
    from master.async_downloader import AsyncDownloader
    downloader = AsyncDownloader()
    downloader.start()
    import time
    time.sleep(2)
    downloader.add('69d6242a-68f7-4768-84d0-d127e0ca87bf', 'test/mrna.fa')
    downloader.add('69d6242a-68f7-4768-84d0-d127e0ca87bf', 'job-69d6242a-68f7-4768-84d0-d127e0ca87bf/output/14')
    time.sleep(1)
    print 'pausing:'
    remaining = downloader.pause('69d6242a-68f7-4768-84d0-d127e0ca87bf')
    print 'Paused!!\nRemaining:'
    print remaining
    time.sleep(1)
    print 'Resuming:'
    downloader.resume()
    print 'Resumed!!'

if __name__ == '__main__':
    cli()
