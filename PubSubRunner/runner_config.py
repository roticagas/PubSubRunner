import os


class RunnerConfig:
    DEFAULT_CONFIG = [
        ('PORT', '8080'),
        ('CLOUD_PROJECT', ''),
        ('CLOUD_PUBSUB_SUBSCRIBE_TOPIC', 'from'),
        ('CLOUD_PUBSUB_SUBSCRIBE_SUBSCRIPTION', 'from-subscription'),
        ('CLOUD_PUBSUB_PUBLISH_TOPIC', ''),
        ('CLOUD_PUBSUB_DEAD_LETTER_TOPIC', ''),
        ('CLOUD_PUBSUB_MAX_LEASE_DURATION', '7200'),
        ('CLOUD_PUBSUB_MAX_DEADLINE', '600'),
        ('CLOUD_PUBSUB_CHECK', 'true'),
        ('CLOUD_PUBSUB_ACK', 'true'),
        ('CLOUD_PUBSUB_DEAD_LETTER_ACK', 'false'),
    ]

    def __init__(self):
        env = [os.getenv(name, default_value) for (name, default_value) in self.DEFAULT_CONFIG]
        self.port = int(env[0])
        self.cloud_project = env[1]
        self.cloud_pubsub_subscribe_topic = env[2]
        self.cloud_pubsub_subscribe_subscription = env[3]
        self.cloud_pubsub_publish_topic = env[4]
        self.cloud_pubsub_dead_letter_topic = env[5]
        self.cloud_pubsub_max_lease_duration = int(env[6])
        self.cloud_pubsub_ack_deadline = int(env[7])
        self.cloud_pubsub_check = env[8] in ['True', 'true', 'TRUE']
        self.cloud_pubsub_ack = env[9] in ['True', 'true', 'TRUE']
        self.cloud_pubsub_dead_letter_ack = env[10] in ['True', 'true', 'TRUE']
