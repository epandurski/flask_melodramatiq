import pytest
import flask
import dramatiq
from flask_melodramatiq import LazyActor, StubBroker, Broker
from mock import Mock


@pytest.fixture
def app(request):
    app = flask.Flask(request.module.__name__)
    app.testing = True
    # app.config['DRAMATIQ_BROKER_URL'] = 'stub://'
    # app.config['DRAMATIQ_BROKER_CLASS'] = 'StubBroker'
    return app


@pytest.fixture
def broker(app, request):
    broker = StubBroker()
    yield broker
    config_prefix = broker._LazyBrokerMixin__config_prefix
    type(broker)._LazyBrokerMixin__registered_config_prefixes.remove(config_prefix)


@pytest.fixture
def configurable_broker(app, request):
    broker = Broker()
    yield broker
    config_prefix = broker._LazyBrokerMixin__config_prefix
    type(broker)._LazyBrokerMixin__registered_config_prefixes.remove(config_prefix)


@pytest.fixture
def run_mock():
    return Mock()


@pytest.fixture
def broker_task(broker, run_mock):
    @broker.actor
    def broker_task(*args, **kwargs):
        run_mock(*args, **kwargs)
    return broker_task


@pytest.fixture
def dramatiq_task(broker, run_mock):
    @dramatiq.actor(broker=broker, actor_class=LazyActor)
    def dramatiq_task(*args, **kwargs):
        run_mock(*args, **kwargs)
    return dramatiq_task
