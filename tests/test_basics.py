import flask
import dramatiq
import pytest
from flask_melodramatiq import Broker, StubBroker
from flask_melodramatiq.lazy_broker import LazyActor, MultipleAppsWarningMiddleware, missing
from dramatiq.middleware import Middleware
from dramatiq.results.backends.redis import RedisBackend
from dramatiq.results.backends.stub import StubBackend
from dramatiq.results import Results
from mock import Mock


def test_actor_attr_access(app, broker, run_mock):
    @broker.actor()
    def task():
        run_mock()

    assert isinstance(str(task), str)
    assert isinstance(repr(task), str)
    task.some_random_attribute = 1
    del task.some_random_attribute
    with pytest.raises(RuntimeError, match=r'init_app\(\) must be called'):
        task.some_random_attribute
    task()
    run_mock.assert_called_once()
    run_mock.reset_mock()
    broker.init_app(app)
    assert isinstance(str(task), str)
    assert isinstance(repr(task), str)
    task.other_random_attribute = 2
    del task.other_random_attribute
    with pytest.raises(AttributeError):
        task.other_random_attribute
    task()
    run_mock.assert_called_once()


def test_immediate_init(app, run_mock):
    broker = StubBroker(app, config_prefix='IMMEDIATE_INIT_BROKER')

    @broker.actor
    def task():
        run_mock()

    task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_broker_task(app, broker, broker_task, run_mock):
    with pytest.raises(RuntimeError, match=r'init_app\(\) must be called'):
        broker_task.send()
    broker.init_app(app)
    broker_task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_dramatiq_task(app, broker, dramatiq_task, run_mock):
    with pytest.raises(RuntimeError, match=r'init_app\(\) must be called'):
        dramatiq_task.send()
    broker.init_app(app)
    dramatiq_task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_register_task_after_init(app, broker, run_mock):
    broker.init_app(app)

    @broker.actor
    def task(p1, p2):
        run_mock(p1, p2)

    task.send('param1', 'param2')
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once_with('param1', 'param2')


def test_multiple_init(app, broker):
    # invalid class name
    app0 = flask.Flask('zero_app')
    app0.testing = True
    app0.config['DRAMATIQ_BROKER_CLASS'] = 'InvalidClassName'
    with pytest.raises(ValueError, match=r'[Ii]nvalid broker class'):
        broker.init_app(app0)

    broker.init_app(app)
    broker.init_app(app)
    broker.init_app(app)

    # second app with the same config
    app2 = flask.Flask('second_app')
    app2.testing = True
    app2.config = app.config
    assert not [1 for m in broker.middleware if type(m) is MultipleAppsWarningMiddleware]
    broker.init_app(app2)
    assert [1 for m in broker.middleware if type(m) is MultipleAppsWarningMiddleware]

    # second app with a different config
    app3 = flask.Flask('third_app')
    app3.testing = True
    app3.config['DRAMATIQ_BROKER_URL'] = 'some_url'
    with pytest.raises(RuntimeError, match=r'reconfigure an already configured broker'):
        broker.init_app(app3)


def test_invalid_actor_arguments(app, broker):
    with pytest.raises(TypeError, match='unexpected keyword argument'):
        @broker.actor(actor_class=None)
        def task1():
            pass
    with pytest.raises(TypeError, match='unexpected keyword argument'):
        @broker.actor(broker=None)
        def task2():
            pass


def test_config_prefix_conflict(app, broker):
    with pytest.raises(RuntimeError, match=r'second broker with configuration prefix'):
        StubBroker()
    with pytest.raises(ValueError, match=r'[Ii]nvalid configuration prefix'):
        StubBroker(config_prefix='not_uppercase')
    second_prefix = 'ANOTHER_{}'.format(type(broker).__name__).upper()
    second_broker = StubBroker(config_prefix=second_prefix)
    assert broker is not second_broker
    broker.init_app(app)
    second_broker.init_app(app)


def test_lazy_actor(app, run_mock):
    import dramatiq.brokers.stub
    broker = dramatiq.brokers.stub.StubBroker()

    @dramatiq.actor(actor_class=LazyActor, broker=broker)
    def task():
        run_mock()

    task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_flask_app_context(app, broker, run_mock):
    @broker.actor
    def task():
        assert app.config is flask.current_app.config
        run_mock()

    broker.init_app(app)
    assert [1 for m in broker.middleware if type(m).__name__ == 'AppContextMiddleware']
    task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_generic_actor(app, broker, run_mock):
    class AbstractTask(dramatiq.GenericActor):
        class Meta:
            abstract = True

        def perform(self, arg):
            self.run(arg)

    class Task(AbstractTask):
        def run(self, arg):
            run_mock(arg)

    broker.init_app(app)
    Task.send('message')
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once_with('message')


def test_config_override(app, caplog):
    # passing options to the constructor
    broker1 = StubBroker(config_prefix='CONFIG_OVERRIDE_BROKER1', some_arg='something')
    with pytest.raises(TypeError, match=r'some_arg'):
        broker1.init_app(app)

    # passing options via app.config
    broker2 = StubBroker(config_prefix='CONFIG_OVERRIDE_BROKER2')
    app.config['CONFIG_OVERRIDE_BROKER2_SOME_ARG'] = 'something'
    with pytest.raises(TypeError, match=r'some_arg'):
        broker2.init_app(app)

    # overriding options via app.config
    broker3 = StubBroker(config_prefix='CONFIG_OVERRIDE_BROKER3', some_arg='something')
    app.config['CONFIG_OVERRIDE_BROKER3_SOME_ARG'] = 'something_else'
    n = len(caplog.records)
    with pytest.raises(TypeError, match=r'some_arg'):
        broker3.init_app(app)
    assert len(caplog.records) == n + 1
    assert 'something_else' in caplog.text

    # setting the broker class via app.config
    broker4 = Broker(config_prefix='CONFIG_OVERRIDE_BROKER4')
    app.config['CONFIG_OVERRIDE_BROKER4_CLASS'] = 'StubBroker'
    assert type(broker4) is Broker
    broker4.init_app(app)
    assert type(broker4) is StubBroker

    # setting the broker class and adding options via app.config
    broker5 = Broker(config_prefix='CONFIG_OVERRIDE_BROKER5')
    app.config['CONFIG_OVERRIDE_BROKER5_CLASS'] = 'StubBroker'
    app.config['CONFIG_OVERRIDE_BROKER5_SOME_ARG'] = 'something'
    with pytest.raises(TypeError, match=r'some_arg'):
        broker5.init_app(app)

    # configurable broker class, passing options to the constructor (failure)
    broker6 = Broker(config_prefix='CONFIG_OVERRIDE_BROKER6', some_arg='something')
    app.config['CONFIG_OVERRIDE_BROKER6_CLASS'] = 'StubBroker'
    with pytest.raises(TypeError, match=r'some_arg'):
        broker6.init_app(app)

    # configurable broker class, passing options to the constructor (success)
    broker7 = Broker(config_prefix='CONFIG_OVERRIDE_BROKER7', middleware=[])
    app.config['CONFIG_OVERRIDE_BROKER7_CLASS'] = 'StubBroker'
    broker7.init_app(app)
    assert type(broker7) is StubBroker
    assert len(broker7.middleware) <= 1

    # alter broker middleware in app configuration
    broker8 = StubBroker(config_prefix='CONFIG_OVERRIDE_BROKER8')
    app.config['CONFIG_OVERRIDE_BROKER8_MIDDLEWARE'] = []
    n = len(caplog.records)
    broker8.init_app(app)
    assert len(caplog.records) == n + 1
    assert 'Ignored configuration setting' in caplog.text

    # options being None are ignored
    broker9 = StubBroker(config_prefix='CONFIG_OVERRIDE_BROKER9')
    app.config['CONFIG_OVERRIDE_BROKER9_CLASS'] = 'StubBroker'
    app.config['CONFIG_OVERRIDE_BROKER9_URL'] = missing
    broker9.init_app(app)
    assert isinstance(broker9, StubBroker)

def test_import_error(app):
    from flask_melodramatiq import create_broker_class
    NonExistingBroker = create_broker_class('non_existing_modlule:NonExistingBroker')
    with pytest.raises(ImportError):
        NonExistingBroker(config_prefix='IMPORT_ERROR_BROKER')


def test_after_process_boot_warning(broker, caplog):
    n = len(caplog.records)
    MultipleAppsWarningMiddleware().after_process_boot(broker)
    assert len(caplog.records) == n + 1
    assert 'application context may be set incorrectly' in caplog.text


def test_add_middleware(app, broker, run_mock):
    class TestMiddleware(Middleware):
        @property
        def actor_options(self):
            return {"test_option"}

        def after_process_message(self, broker, message, *, result=None, exception=None):
            run_mock()

    broker.add_middleware(TestMiddleware())

    @broker.actor(test_option=123)
    def task():
        pass

    broker.init_app(app)
    broker.add_middleware(TestMiddleware())
    assert task.options['test_option'] == 123
    task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    assert run_mock.call_count == 2


class TestBackendResultMiddleware:
    def test_it_remove_the_dummy_backend_at_init_app(self, app, broker):
        mocked_client = Mock()
        backend = RedisBackend(client=mocked_client)
        result_middleware = Results(backend=backend)
        broker.add_middleware(result_middleware)
        broker.init_app(app)

        only_results = list(filter(lambda x: isinstance(x, Results), broker.middleware))
        assert len(only_results) == 1
        assert only_results[0].backend is backend

    def test_it_can_lazy_register_actors_with_store_results(self, app, broker, run_mock):
        expected = "HelloYou"
        run_mock.return_value = expected
        @broker.actor(store_results=True)
        def task():
            return run_mock()

        backend = StubBackend()
        result_middleware = Results(backend=backend)
        broker.add_middleware(result_middleware)
        broker.init_app(app)

        job = task.send()
        worker = dramatiq.Worker(broker)
        worker.start()
        worker.join()
        assert job.get_result() == expected

    def test_it_raises_when_at_runtime_when_no_backend_for_results(self, app, broker):
        @broker.actor(store_results=True)
        def task():
            pass

        broker.init_app(app)
        broker.emit_after("process_boot")  # set Prometheus

        job = task.send()
        worker = dramatiq.Worker(broker)
        worker.start()
        worker.join()

        match = "default broker doesn't have a results backend"
        with pytest.raises(RuntimeError, match=match):
            job.get_result()
