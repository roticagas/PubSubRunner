import http.server
import json
import logging
import socketserver
from json import JSONDecodeError

from cloud_util import CloudUtil
from runner_config import RunnerConfig


class RunnerApplication:

    def __init__(self, task):
        self.task = task
        self.config = RunnerConfig()
        self.create_pubsub()
        assert self.config.cloud_project != '', 'Please set cloud project'
        assert self.config.cloud_pubsub_subscribe_subscription != '', 'Please set cloud pubsub subscription'
        assert self.config.cloud_pubsub_dead_letter_topic != '', 'Please set cloud pubsub topic for dead letter'
        if self.config.cloud_pubsub_publish_topic == '':
            logging.warning('publish topic are not set, check your config carefully')

    def create_pubsub(self):
        if self.config.cloud_check:
            cloud_project = self.config.cloud_project
            subscribe_topic = self.config.cloud_pubsub_subscribe_topic
            subscribe_subscription = self.config.cloud_pubsub_subscribe_subscription
            publish_topic = self.config.cloud_pubsub_publish_topic
            dead_letter_topic = self.config.cloud_pubsub_dead_letter_topic
            ack_deadline = self.config.cloud_pubsub_ack_deadline
            assert (subscribe_topic != publish_topic)
            if not CloudUtil.verify_topic(cloud_project, subscribe_topic):
                CloudUtil.create_topic(cloud_project, subscribe_topic)
            if not CloudUtil.verify_subscription(cloud_project, subscribe_subscription):
                CloudUtil.create_subscription(cloud_project, subscribe_topic, subscribe_subscription, ack_deadline)
            if not CloudUtil.verify_topic(cloud_project, publish_topic):
                CloudUtil.create_topic(cloud_project, publish_topic)
            if not CloudUtil.verify_topic(cloud_project, dead_letter_topic):
                CloudUtil.create_topic(cloud_project, dead_letter_topic)

    def do_task(self, data):
        return self.task(json.loads(data))

    def subscribe_processing(self, message):
        try:
            ret = self.do_task(message.data)
            if self.config.cloud_pubsub_publish_topic != '':
                CloudUtil.publish_data(self.config.cloud_project,
                                       self.config.cloud_pubsub_publish_topic,
                                       json.dumps(ret))
            message.ack()
        except JSONDecodeError as e:
            # NOTE: if payload not in json format: send to dead letter topic
            logging.error(str(e))
            CloudUtil.publish_dead_letter(self.config.cloud_project,
                                          self.config.cloud_pubsub_dead_letter_topic,
                                          self.config.cloud_pubsub_publish_topic,
                                          message,
                                          e,
                                          'Invalid json format')
            message.ack()
        except Exception as e:
            logging.error(str(e))
            message.nack()

    def run(self, job=None):
        """
        subscribe message from pubsub and may do a job before httpd.serve_forever()
        :param job: function to run before run httpd.serve_forever()
        """
        CloudUtil.subscribe_message(self.config.cloud_project,
                                    self.config.cloud_pubsub_subscribe_subscription,
                                    self.subscribe_processing,
                                    self.config.cloud_pubsub_max_lease_duration)
        if job is not None and callable(job):
            job()
        with socketserver.TCPServer(("", self.config.port), http.server.SimpleHTTPRequestHandler) as httpd:
            logging.info("serving at port {}".format(self.config.port))
            httpd.serve_forever()
