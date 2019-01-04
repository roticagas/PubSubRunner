import os
import unittest
from json import JSONDecodeError

from runner_application import RunnerApplication
from runner_config import RunnerConfig


class TestRunnerApplication(unittest.TestCase):

    def test_runner_config(self):
        main_config = RunnerConfig()
        self.assertTrue(main_config.cloud_check is not None)
        self.assertTrue(main_config.cloud_pubsub_subscribe_topic is not None)
        self.assertTrue(main_config.cloud_pubsub_subscribe_subscription is not None)
        self.assertTrue(main_config.cloud_pubsub_publish_topic is not None)
        self.assertTrue(main_config.cloud_pubsub_dead_letter_topic is not None)
        self.assertTrue(main_config.cloud_pubsub_max_lease_duration is not None)

    @staticmethod
    def _empty_task(message):
        return message

    def test_main_app(self):
        os.environ['CLOUD_CHECK'] = 'false'
        os.environ['CLOUD_PROJECT'] = 'project'
        m = RunnerApplication(self._empty_task)
        m.do_task(str(u'{}'))

    def test_main_app_return(self):
        os.environ['CLOUD_CHECK'] = 'false'
        os.environ['CLOUD_PROJECT'] = 'project'
        m = RunnerApplication(self._empty_task)
        e = m.do_task(str(u'{"hello": "world"}'))
        self.assertEqual(e['hello'], 'world')

    def test_main_app_decode_error(self):
        with self.assertRaises(JSONDecodeError):
            os.environ['CLOUD_CHECK'] = 'false'
            os.environ['CLOUD_PROJECT'] = 'project'
            m = RunnerApplication(self._empty_task)
            m.do_task(str(u'10-5'))


if __name__ == '__main__':
    unittest.main()
