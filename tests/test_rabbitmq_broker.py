import dramatiq
import time


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
