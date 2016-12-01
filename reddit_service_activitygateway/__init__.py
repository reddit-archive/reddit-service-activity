import hashlib
import logging

from baseplate import Baseplate, make_metrics_client, config
from baseplate.context.thrift import ThriftContextFactory
from baseplate.integration.pyramid import BaseplateConfigurator
from baseplate.thrift_pool import ThriftConnectionPool
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPServiceUnavailable, HTTPNoContent

from reddit_service_activity.activity_thrift import ActivityService


logger = logging.getLogger(__name__)


class ActivityGateway(object):
    def is_healthy(self, request):
        try:
            if request.activity.is_healthy():
                return {
                    "status": "healthy",
                }
        except:
            logger.exception("Failed health check")
            raise HTTPServiceUnavailable()

    def pixel(self, request):
        context_id = request.matchdict["context_id"]
        user_agent = (request.user_agent or '').encode("utf8")
        remote_addr = request.remote_addr.encode()
        visitor_id = hashlib.sha1(remote_addr + user_agent).hexdigest()

        request.activity.record_activity(context_id, visitor_id)

        return HTTPNoContent(
            headers={
                "Cache-Control": "no-cache, max-age=0",
                "Pragma": "no-cache",
                "Expires": "Thu, 01 Jan 1970 00:00:00 GMT",
            }
        )


def make_wsgi_app(app_config):
    cfg = config.parse_config(app_config, {
        "activity": {
            "endpoint": config.Endpoint,
        },
    })

    metrics_client = make_metrics_client(app_config)

    pool = ThriftConnectionPool(cfg.activity.endpoint)

    baseplate = Baseplate()
    baseplate.configure_logging()
    baseplate.configure_metrics(metrics_client)
    baseplate.add_to_context("activity",
        ThriftContextFactory(pool, ActivityService.Client))

    configurator = Configurator(settings=app_config)

    baseplate_configurator = BaseplateConfigurator(baseplate)
    configurator.include(baseplate_configurator.includeme)

    controller = ActivityGateway()
    configurator.add_route("health", "/health", request_method="GET")
    configurator.add_view(controller.is_healthy, route_name="health", renderer="json")

    configurator.add_route("pixel", "/{context_id:[A-Za-z0-9_]{,40}}.png",
                           request_method="GET")
    configurator.add_view(controller.pixel, route_name="pixel")

    return configurator.make_wsgi_app()
