import http.server
import json
import logging
import socketserver
from json import JSONDecodeError

from PubSubRunner.cloud_util import CloudUtil
from PubSubRunner.runner_config import RunnerConfig


class RunnerApplication:

    def __init__(self, task):
        self.task = task
        self.config = RunnerConfig()
        self.verify_config()
        self.check_pubsub()

    def verify_config(self):
        """
        Verify config
        """
        assert self.config.cloud_project != '', 'Please set cloud project'
        assert self.config.cloud_pubsub_subscribe_subscription != '', 'Please set cloud pubsub subscription'
        if self.config.cloud_pubsub_publish_topic == '':
            logging.warning('publish topic are not set, check your config carefully')
        if self.config.cloud_pubsub_dead_letter_topic == '':
            logging.warning('dead letter topic are not set, check your config carefully')
            assert not self.config.cloud_pubsub_dead_letter_ack, 'dead letter should not be sent if message not ack'

    def check_pubsub(self):
        """
        Check pubsub if not exist: create them
        """
        if self.config.cloud_pubsub_check:
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
            if publish_topic != '':  # optional topic
                if not CloudUtil.verify_topic(cloud_project, publish_topic):
                    CloudUtil.create_topic(cloud_project, publish_topic)
            if dead_letter_topic != '':  # optional topic
                if not CloudUtil.verify_topic(cloud_project, dead_letter_topic):
                    CloudUtil.create_topic(cloud_project, dead_letter_topic)

    def do_task(self, data):
        """
        wrapper for call task
        :param data: raw message from pubsub expect json format
        :return: json of return value or data if return value is None
        """
        ret = self.task(json.loads(data))
        return json.dumps(ret or data)

    def subscribe_processing(self, message):
        try:
            ret = self.do_task(message.data)
            if self.config.cloud_pubsub_publish_topic != '':
                CloudUtil.publish_data(self.config.cloud_project,
                                       self.config.cloud_pubsub_publish_topic,
                                       ret)
            if self.config.cloud_pubsub_ack:
                message.ack()
        except JSONDecodeError as e:
            # NOTE: if payload not in json format: send to dead letter topic
            logging.error(str(e))
            if self.config.cloud_pubsub_dead_letter_topic != '':
                CloudUtil.publish_dead_letter(self.config.cloud_project,
                                              self.config.cloud_pubsub_dead_letter_topic,
                                              self.config.cloud_pubsub_publish_topic,
                                              message,
                                              e,
                                              'Invalid json format')
                if self.config.cloud_pubsub_dead_letter_ack:
                    message.ack()
        except Exception as e:
            logging.error(str(e))
            message.nack()

    def run(self, job=None, internal_server=True):
        """
        subscribe message from pubsub and may do a job before httpd.serve_forever()
        :param job: function to run before run httpd.serve_forever()
        :param internal_server: True to use internal TCPServer run after subscribe message
        """
        CloudUtil.subscribe_message(self.config.cloud_project,
                                    self.config.cloud_pubsub_subscribe_subscription,
                                    self.subscribe_processing,
                                    self.config.cloud_pubsub_max_lease_duration)
        if job is not None and callable(job):
            job()
        if internal_server:
            with socketserver.TCPServer(("", self.config.port), http.server.SimpleHTTPRequestHandler) as httpd:
                logging.info("serving at port {}".format(self.config.port))
                httpd.serve_forever()
