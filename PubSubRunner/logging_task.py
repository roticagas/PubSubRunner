import logging


def logging_task(message):
    logging.debug('task received: {}'.format(message))

#  from runner_application import RunnerApplication
#
#  if __name__ == '__main__':
#     RunnerApplication(logging_task).run()
#
