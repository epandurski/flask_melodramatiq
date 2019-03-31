import pytest
import flask
import dramatiq
from flask_melodramatiq.lazy_broker import _registered_config_prefixes
from flask_melodramatiq import StubBroker, Broker, RabbitmqBroker
from mock import Mock


@pytest.fixture
def app(request):
    app = flask.Flask(request.module.__name__)
    app.testing = True
    return app


@pytest.fixture(params=['StubBroker', 'Broker'])
def broker(app, request):
    if request.param == 'StubBroker':
        broker = StubBroker()
    else:
        app.config['DRAMATIQ_BROKER_CLASS'] = 'StubBroker'
        broker = Broker()
    broker.set_default()
    yield broker
    config_prefix = broker._LazyBrokerMixin__config_prefix
    _registered_config_prefixes.remove(config_prefix)


@pytest.fixture
def rabbitmq_broker(app, request):
    broker = RabbitmqBroker()
    broker.set_default()
    yield broker
    config_prefix = broker._LazyBrokerMixin__config_prefix
    _registered_config_prefixes.remove(config_prefix)


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
    @dramatiq.actor(broker=broker)
    def dramatiq_task(*args, **kwargs):
        run_mock(*args, **kwargs)
    return dramatiq_task
