import global_conf, click

@click.group()
def cli():
    pass

@cli.command()
def assume_worker():
    from worker import Worker
    w=Worker()
    w.run()

@cli.command()
def provision_workers():
    from master.worker_manager import launch_workers
    launch_workers()

@cli.command()
def assume_master():
    from master import Master
    master=Master()
    master.start()

@cli.command()
def delete_worker_messages():
    from aws.sqs import workers_messaging_queue
    workers_messaging_queue.delete_all_messages()

@cli.command()
def delete_task_messages():
    from aws.sqs import new_tasks_queue
    new_tasks_queue.delete_all_messages()

if __name__ == '__main__':
    cli()
