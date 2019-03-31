import dramatiq
import time


def test_broker_connection_attrs(app, rabbitmq_broker):
    rabbitmq_broker.init_app(app)
    assert hasattr(rabbitmq_broker, 'channel')
    assert hasattr(rabbitmq_broker, 'connection')
    del rabbitmq_broker.channel
    del rabbitmq_broker.connection
    assert hasattr(rabbitmq_broker, 'channel')
    assert hasattr(rabbitmq_broker, 'connection')


def test_connection_closed_error():
    e = Exception()
    assert isinstance(dramatiq.ConnectionClosed(e), Exception)


def test_publish_message(app, rabbitmq_broker, run_mock):
    @rabbitmq_broker.actor
    def task():
        run_mock()

    rabbitmq_broker.init_app(app)
    m = dramatiq.Message(queue_name='default', actor_name='task', args=(), kwargs={}, options={})
    rabbitmq_broker.publish_message(m, exchange='')
    worker = dramatiq.Worker(rabbitmq_broker)
    worker.start()
    time.sleep(2.0)
    worker.join()
    worker.stop()
    run_mock.assert_called_once()

    # try `queue_name=None`
    m = dramatiq.Message(queue_name=None, actor_name='task', args=(), kwargs={}, options={})
    rabbitmq_broker.publish_message(m, exchange='')
