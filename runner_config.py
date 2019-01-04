import os


class RunnerConfig:
    DEFAULT_PORT = '8080'
    DEFAULT_CLOUD_PROJECT = ''
    DEFAULT_CLOUD_PUBSUB_SUBSCRIBE_TOPIC = 'from'
    DEFAULT_CLOUD_PUBSUB_SUBSCRIBE_SUBSCRIPTION = 'from-subscription'
    DEFAULT_CLOUD_PUBSUB_PUBLISH_TOPIC = 'to'
    DEFAULT_CLOUD_PUBSUB_DEAD_LETTER_TOPIC = 'dead-letter'
    DEFAULT_CLOUD_PUBSUB_MAX_LEASE_DURATION = '7200'
    DEFAULT_CLOUD_PUBSUB_ACK_DEADLINE = '600'

    def __init__(self):
        self.port = int(os.getenv('PORT', self.DEFAULT_PORT))
        self.cloud_project = os.getenv('CLOUD_PROJECT', self.DEFAULT_CLOUD_PROJECT)
        self.cloud_pubsub_subscribe_topic = os.getenv('CLOUD_PUBSUB_SUBSCRIBE_TOPIC',
                                                      self.DEFAULT_CLOUD_PUBSUB_SUBSCRIBE_TOPIC)
        self.cloud_pubsub_subscribe_subscription = os.getenv('CLOUD_PUBSUB_SUBSCRIBE_SUBSCRIPTION',
                                                             self.DEFAULT_CLOUD_PUBSUB_SUBSCRIBE_SUBSCRIPTION)
        self.cloud_pubsub_publish_topic = os.getenv('CLOUD_PUBSUB_PUBLISH_TOPIC',
                                                    self.DEFAULT_CLOUD_PUBSUB_PUBLISH_TOPIC)
        self.cloud_pubsub_dead_letter_topic = os.getenv('CLOUD_PUBSUB_DEAD_LETTER_TOPIC',
                                                        self.DEFAULT_CLOUD_PUBSUB_DEAD_LETTER_TOPIC)
        self.cloud_pubsub_max_lease_duration = int(os.getenv('CLOUD_PUBSUB_MAX_LEASE_DURATION',
                                                             self.DEFAULT_CLOUD_PUBSUB_MAX_LEASE_DURATION))
        self.cloud_pubsub_ack_deadline = int(os.getenv('CLOUD_PUBSUB_MAX_DEADLINE',
                                                       self.DEFAULT_CLOUD_PUBSUB_ACK_DEADLINE))
        self.cloud_check = os.getenv('CLOUD_CHECK', 'true') in ['True', 'true']
