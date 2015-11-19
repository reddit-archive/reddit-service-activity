import unittest

import mock
import webtest

from pyramid.httpexceptions import HTTPException

from reddit_service_activity.activity_thrift.ActivityService import Client
from reddit_service_activitygateway import ActivityGateway, make_wsgi_app


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.request = mock.Mock()
        self.request.activity = mock.Mock(spec=Client)
        self.controller = ActivityGateway()

    def test_health_check(self):
        self.controller.is_healthy(self.request)

        self.assertTrue(self.request.activity.is_healthy.called)

    def test_health_check_failed(self):
        self.request.activity.is_healthy.side_effect = Exception

        with self.assertRaises(HTTPException):
            self.controller.is_healthy(self.request)

    def test_pixel(self):
        self.request.matchdict = {"context_id": "context"}
        self.request.user_agent = "Mozilla/5.0"
        self.request.remote_addr = "1.2.3.4"

        self.controller.pixel(self.request)

        self.request.activity.record_activity.assert_called_with(
            "context", "6abbd3bc1a661ad396626b8c77b2ba6e52943782")


class GatewayFunctionalTests(unittest.TestCase):
    @mock.patch("reddit_service_activitygateway.ThriftConnectionPool")
    @mock.patch("reddit_service_activitygateway.make_metrics_client")
    def setUp(self, metrics_client, ThriftConnectionPool):
        app = make_wsgi_app({
            "activity.endpoint": "/socket",
            "metrics.endpoint": "/socket",
            "metrics.namespace": "namespace",
        })

        self.test_app = webtest.TestApp(app)

    def test_ping(self):
        self.test_app.get("/context.png", extra_environ={"REMOTE_ADDR": "1.2.3.4"})

    def test_invalid_url(self):
        self.test_app.get("/this-context-is-invalid.png", status=404)
