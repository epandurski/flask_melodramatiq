import flask
import dramatiq
import pytest
from flask_melodramatiq import LazyActor, StubBroker, AppContextMiddleware, MultipleAppsWarningMiddleware


def test_immediate_init(app, run_mock):
    broker = StubBroker(app, config_prefix='TEST_IMMEDIATE_INIT_BROKER')

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
    assert [1 for m in broker.middleware if type(m) is AppContextMiddleware]
    task.send()
    worker = dramatiq.Worker(broker)
    worker.start()
    worker.join()
    run_mock.assert_called_once()


def test_generic_actor(app, broker, run_mock):
    class AbstractTask(dramatiq.GenericActor):
        class Meta:
            abstract = True
            actor_class = LazyActor

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
