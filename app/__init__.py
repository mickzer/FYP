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
def provision_job(name, executable_key_path, input_key_path):
     from master.job_manager import  create_job, submit_job
     job_id = create_job(name=name, executable_key_path=executable_key_path, input_key_path=input_key_path, export_key_path='/none')
     submit_job(job_id)
     from master.receiver import receive
     log.info('Job Submitted, Starting Receiver')
     receive()

@cli.command()
def receive():
    from master.receiver import receive
    log.info('Starting Receiver')
    receive()

if __name__ == '__main__':
    cli()
