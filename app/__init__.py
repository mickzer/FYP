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

if __name__ == '__main__':
    cli()
