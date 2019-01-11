import logging
from pathlib import Path

from google.cloud import storage, pubsub_v1
from google.cloud.exceptions import Forbidden
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.storage import Bucket
from google.cloud.storage.notification import JSON_API_V1_PAYLOAD_FORMAT


class CloudUtil:
    @staticmethod
    def lookup_bucket(project_id, bucket_name):
        """
        :param project_id:
        :param bucket_name:
        :return: True if bucket exist
        """
        logging.debug('HINT: gsutil ls gs://{}'.format(bucket_name))
        # Initialise a client
        storage_client = storage.Client(project_id)
        bucket = storage_client.lookup_bucket(bucket_name)
        logging.debug('bucket : {}'.format(bucket))
        ret = isinstance(bucket, Bucket)
        logging.debug('lookup_bucket : {}'.format(ret))
        if ret:
            logging.info('lookup_bucket: ok')
        return ret

    @staticmethod
    def create_bucket(project_id, bucket_name):
        """
        :param project_id:
        :param bucket_name:
        """
        logging.debug('HINT: gsutil mb gs://{}'.format(bucket_name.split('.')[0]))
        logging.debug('HINT: BucketNotFoundException if not found')
        # Initialise a client
        storage_client = storage.Client(project_id)
        bucket = storage_client.create_bucket(bucket_name.split('.')[0])
        assert (isinstance(bucket, Bucket))
        logging.info('create_bucket : ok')

    @staticmethod
    def verify_acl(project_id, bucket_name, role='OWNER'):
        """
        :param project_id:
        :param bucket_name:
        :param role: OWNER, READER, WRITER
        """
        logging.debug('HINT: gsutil acl get gs://{}'.format(bucket_name))
        # Initialise a client
        storage_client = storage.Client(project_id)
        bucket = storage_client.get_bucket(bucket_name)
        assert (isinstance(bucket, Bucket))

        entities = bucket.acl.get_entities()
        logging.debug('entities : {}'.format(entities))
        for e in entities:
            logging.debug('entity : {}'.format(e))
        has_project_role = len([e for e in entities if e.type == 'project' and role in e.roles]) >= 1
        has_user_role = len([e for e in entities if e.type == 'user' and role in e.roles]) >= 1
        assert (has_project_role or has_user_role)
        logging.info('verify_acl : ok')

    @staticmethod
    def set_acl(project_id, bucket_name, role='OWNER'):
        """
        :param project_id:
        :param bucket_name:
        :param role: OWNER, READER, WRITER
        """
        storage_client = storage.Client(project_id)
        service_account_email = storage_client.get_service_account_email()
        role_access = {'OWNER': 'O', 'WRITER': 'W', 'READER': 'R'}
        logging.debug(
            'HINT: gsutil acl ch -g {}:{} gs://{}'.format(service_account_email, role_access[role], bucket_name))

        """Adds a user as an role on the given bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Reload fetches the current ACL from Cloud Storage.
        bucket.acl.reload()

        # You can also use `group()`, `domain()`, `all_authenticated()` and `all()`
        # to grant access to different types of entities.
        # You can also use `grant_read()` or `grant_write()` to grant different
        # roles.
        if role == 'OWNER':
            bucket.acl.user(service_account_email).grant_owner()
            bucket.acl.save()
        elif role == 'READER':
            bucket.acl.user(service_account_email).grant_read()
            bucket.acl.save()
        elif role == 'WRITER':
            bucket.acl.user(service_account_email).grant_write()
            bucket.acl.save()

        logging.info('Added user {} as an {} on bucket {}.'.format(
            service_account_email, role, bucket_name))

    @staticmethod
    def verify_topic(project_id, topic_name):
        """
        :param project_id:
        :param topic_name:
        :return: True if topic exist
        """
        logging.debug('HINT: gcloud pubsub topics list')
        publisher = pubsub_v1.PublisherClient()
        project_path = publisher.project_path(project_id)
        topic_path = publisher.topic_path(project_id, topic_name)

        for topic in publisher.list_topics(project_path):
            logging.debug('topic : {}'.format(topic))
            if topic.name == topic_path:
                logging.info('verify_topic: ok')
                return True
        return False

    @staticmethod
    def list_topic(project_id):
        """
        :param project_id:
        :return: list of topic
        """
        logging.debug('HINT: gcloud pubsub topics list')
        publisher = pubsub_v1.PublisherClient()
        project_path = publisher.project_path(project_id)

        return publisher.list_topics(project_path)

    @staticmethod
    def create_topic(project_id, topic_name):
        """
        :param project_id:
        :param topic_name:
        """
        logging.debug('HINT: gcloud pubsub topics create {}'.format(topic_name))
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)

        topic = publisher.create_topic(topic_path)
        assert (topic is not None)
        logging.debug('Topic created: {}'.format(topic))
        logging.info('create_topic: ok')

    @staticmethod
    def verify_subscription(project_id, subscription_name):
        """
        :param project_id:
        :param subscription_name:
        :return: True if subscription exist
        """
        logging.debug('HINT: gcloud pubsub subscriptions list')
        subscriber = pubsub_v1.SubscriberClient()
        project_path = subscriber.project_path(project_id)
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        subscriptions = subscriber.list_subscriptions(project_path)
        for s in subscriptions:
            logging.debug('subscription : {}'.format(s))
            if s.name == subscription_path:
                logging.info('verify_subscription: ok')
                return True
        return False

    @staticmethod
    def create_subscription(project_id, topic_name, subscription_name, ack_deadline_seconds=600):
        """
        :param project_id:
        :param topic_name:
        :param subscription_name:
        :param ack_deadline_seconds: 600 is default max ack deadline 10 min
        """
        logging.debug('HINT: gcloud pubsub subscriptions create --ack-deadline 600 --topic {} {}'.format(
            topic_name, subscription_name))
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(project_id, topic_name)
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        subscription = subscriber.create_subscription(subscription_path, topic_path,
                                                      ack_deadline_seconds=ack_deadline_seconds)
        assert (subscription is not None)
        logging.debug('Subscription created: {}'.format(subscription))
        logging.info('create_subscription: ok')

    @staticmethod
    def verify_notification(project_id, bucket_name, topic_name):
        """
        :param project_id:
        :param bucket_name:
        :param topic_name:
        :return: True if  notification exist
        """
        logging.debug('HINT: gsutil notification list gs://{}'.format(bucket_name))
        logging.debug('HINT: gcloud pubsub topics list')
        storage_client = storage.Client(project_id)
        bucket = storage_client.bucket(bucket_name)
        notifications = bucket.list_notifications()
        for n in notifications:
            if n.topic_name == topic_name and n.topic_project == project_id:
                logging.info('verify_notification : ok')
                return True
        return False

    @staticmethod
    def create_notification(project_id, bucket_name,
                            topic_name, event_type='OBJECT_FINALIZE',
                            payload_format=JSON_API_V1_PAYLOAD_FORMAT):
        """
        :param project_id:
        :param bucket_name:
        :param topic_name:
        :param event_type: OBJECT_FINALIZE
        :param payload_format: JSON_API_V1_PAYLOAD_FORMAT
        """
        logging.debug(
            'HINT: gsutil notification create -f json -e {} -t {} gs://{}'.format(
                event_type,
                topic_name,
                bucket_name))
        logging.debug('TODO: set_notification, still can not add new topic notification')
        storage_client = storage.Client(project_id)
        bucket = storage_client.bucket(bucket_name)

        notification = bucket.notification(topic_name, topic_project=project_id,
                                           event_types=[event_type], payload_format=payload_format)
        try:
            notification.create()  # 403
        except Forbidden as e:
            logging.error('dev: run this command by yourself.')
            logging.error('gsutil notification create -f json -e {} -t {} gs://{}'.format(
                event_type,
                topic_name,
                bucket_name))
            raise e
        logging.info('create_notification : ok')

    @staticmethod
    def subscribe_message(project_id, subscription__name, callback, max_lease_duration=0):
        """
        Receives messages from a pull subscription.
        :param project_id:
        :param subscription__name:
        :param callback:
        :param max_lease_duration: extend deadline as seconds
        """
        logging.info('subscribe')
        subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_name}`
        subscription_path = subscriber.subscription_path(project_id, subscription__name)
        if max_lease_duration > 0:
            flow_control = FlowControl(max_lease_duration=max_lease_duration)
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
        else:
            subscriber.subscribe(subscription_path, callback=callback)
        logging.debug('Listening for messages on {}'.format(subscription_path))

    @staticmethod
    def pull_message(project_id, subscription_name, callback):
        """
        Pulling messages synchronously.
        :param project_id:
        :param subscription_name:
        :param callback:
        """
        logging.debug('HINT: gcloud pubsub subscriptions pull {}'.format(subscription_name))
        logging.debug('HINT: gcloud pubsub subscriptions pull --auto-ack {}'.format(subscription_name))
        logging.debug('pull')
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)

        # The subscriber pulls a specific number of messages.
        response = subscriber.pull(subscription_path, max_messages=1, return_immediately=True)
        logging.debug('response: {}'.format(response))
        ack_ids = []
        for received_message in response.received_messages:
            message = received_message.message
            logging.debug("Received: {}".format(message))
            callback(message)
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        if len(ack_ids) > 0:
            subscriber.acknowledge(subscription_path, ack_ids)
            logging.debug("Received and acknowledged {} messages. Done.".format(ack_ids))

    @staticmethod
    def publish_data(project_id, topic_name, data, callback=None):
        """
        :param project_id:
        :param topic_name:
        :param data:
        :param callback:
        """
        logging.info('publish : {}'.format(data))
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        data = u'{}'.format(data)
        # Data must be a bytestring
        data = data.encode('utf-8')
        logging.debug('HINT: gcloud pubsub topics publish {} --message "{}"'.format(topic_name, data))
        # When you publish a message, the client returns a Future.
        message_future = publisher.publish(topic_path, data=data)
        if callback is not None:
            message_future.add_done_callback(callback)
        message_future.result()
        logging.info('publish : ok')

    @staticmethod
    def publish_dead_letter(project_id, topic_name, subscribe_topic, subscribe_message, subscribe_exception, reason=''):
        """
        :param project_id:
        :param topic_name:
        :param subscribe_topic:
        :param subscribe_message:
        :param subscribe_exception:
        :param reason:
        """
        logging.info('publish dead letter: {}'.format(subscribe_message))
        dead_letter = {'topic': subscribe_topic, 'message': subscribe_message, 'exception': subscribe_exception,
                       'reason': reason}
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        data = u'{}'.format(dead_letter)
        # Data must be a bytestring
        data = data.encode('utf-8')
        logging.debug('HINT: gcloud pubsub topics publish {} --message "{}"'.format(topic_name, data))
        # When you publish a message, the client returns a Future.
        message_future = publisher.publish(topic_path, data=data)
        message_future.result()
        logging.info('publish dead letter: ok')

    @staticmethod
    def download_to_filename(project_id, bucket_name, bucket_path, file_path) -> None:
        """
        :param project_id:
        :param bucket_name:
        :param bucket_path:
        :param file_path:
        """
        logging.debug('HINT: gsutil cp {}/{} {}'.format(bucket_name, bucket_path, file_path))
        # need Storage Viewer and Storage Legacy Bucket Owner
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Initialise a client
        storage_client = storage.Client(project_id)
        # Create a bucket object for our bucket
        bucket = storage_client.get_bucket(bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(bucket_path)
        # Download the file to a destination
        blob.download_to_filename(file_path)
        logging.info('downloaded {}/{} to {}'.format(bucket_name, bucket_path, file_path))
