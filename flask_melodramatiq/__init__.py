import threading
import importlib
import functools
import dramatiq
from dramatiq.broker import Broker as AbstractBroker
from dramatiq.brokers import stub

__all__ = ['LazyActor', 'RabbitmqBroker', 'RedisBroker', 'StubBroker']

_broker_classes = {}


def _create_broker_class(module_name, class_name, default_url):
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        broker_class = type(class_name, (), dict(
            __init__=functools.partial(_raise_error, e),
        ))
    else:
        superclass = getattr(module, class_name)
        broker_class = type(class_name, (_LazyBrokerMixin, superclass), dict(
            _LazyBrokerMixin__broker_default_url=default_url,
            _LazyBrokerMixin__broker_factory=superclass,
        ))
    _broker_classes[class_name] = broker_class
    return broker_class


def _raise_error(e, *args, **kwargs):
    raise e


class _ProxiedInstanceMixin:
    """Delegates attribute access to a lazily created instance.

    The lazily created instance is held in `self._proxied_instance`.

    `object.__setattr__(self, '_proxied_instance', None)` should be
    the first thing executed in the constructor (to avoid infinite
    recursion).

    Setting `self._proxied_instance` to anything different than `None`
    prevents any further attribute assignments on `self` (they will go
    to the proxied instance instead).

    """

    def __str__(self):
        if self._proxied_instance is None:
            return object.__str__(self)
        return str(self._proxied_instance)

    def __repr__(self):
        if self._proxied_instance:
            return repr(self._proxied_instance)
        return object.__repr__(self)

    def __getattr__(self, name):
        if self._proxied_instance is None:
            raise RuntimeError(
                'init_app() must be called on brokers before use. '
                'Did you forget to pass the "app" to broker\'s constructor?'
            )
        return getattr(self._proxied_instance, name)

    def __setattr__(self, name, value):
        if self._proxied_instance is None:
            return object.__setattr__(self, name, value)
        return setattr(self._proxied_instance, name, value)

    def __delattr__(self, name):
        if self._proxied_instance is None:
            return object.__delattr__(self, name)
        return delattr(self._proxied_instance, name)


class _LazyBrokerMixin(_ProxiedInstanceMixin):
    __registered_config_prefixes = set()
    __broker_default_url = None

    def __init__(self, app=None, config_prefix='DRAMATIQ_BROKER', **options):
        object.__setattr__(self, '_proxied_instance', None)
        if not config_prefix.isupper():
            raise ValueError(
                'Invalid configuration prefix: "{}". Configuration prefixes '
                'should be all uppercase.'.format(config_prefix)
            )
        if config_prefix in self.__registered_config_prefixes:
            raise RuntimeError(
                'Can not create a second broker with configuration prefix "{}". '
                'Did you forget to pass the "config_prefix" argument when '
                'creating the broker?'.format(config_prefix)
            )
        self.__registered_config_prefixes.add(config_prefix)
        self.__config_prefix = config_prefix
        self.__options = options
        self.__configuration = None
        self.__app = None
        self.__stub = stub.StubBroker(middleware=options.get('middleware'))
        self._unregistered_lazy_actors = []
        if config_prefix == 'DRAMATIQ_BROKER':
            dramatiq.set_broker(self)
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        configuration = self.__get_configuration(app)
        if self.__stub:
            self.__stub.close()
            self.__stub = None
            self.__app = app
            self.__configuration = configuration
            options = configuration.copy()
            self.__class__ = options.pop('class')
            broker = self.__broker_factory(**options)
            broker.add_middleware(AppContextMiddleware(app))
            for actor in self._unregistered_lazy_actors:
                actor._register_proxied_instance(broker=broker)
            self._unregistered_lazy_actors = None
            self._proxied_instance = broker  # `self` is sealed from now on.
        if configuration != self.__configuration:
            raise RuntimeError(
                '{} tried to reconfigure an already configured broker.'.format(app)
            )
        if app is not self.__app:
            self._proxied_instance.add_middleware(MultipleAppsWarningMiddleware())
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions[self.__config_prefix.lower()] = self

    def actor(self, fn=None, **kw):
        for kwarg in ['broker', 'actor_class']:
            if kwarg in kw:
                raise TypeError("actor() got an unexpected keyword argument '{}'".format(kwarg))

        decorator = dramatiq.actor(actor_class=LazyActor, broker=self, **kw)
        if fn is None:
            return decorator
        return decorator(fn)

    @property
    def actor_options(self):
        return (self._proxied_instance or self.__stub).actor_options

    def __get_primary_options(self):
        assert 'class' not in self.__options
        options = self.__options.copy()
        class_name = type(self).__name__
        if class_name in _broker_classes:
            options['class'] = class_name
        return options

    def __get_secondary_options(self, app):
        prefix = '{}_'.format(self.__config_prefix)
        return {
            k[len(prefix):].lower(): v
            for k, v in app.config.items()
            if k.isupper() and k.startswith(prefix)
        }

    def __merge_options(self, primary, secondary):
        options = primary.copy()
        for k, v in options.items():
            if k in secondary and v != secondary[k]:
                raise ValueError(
                    'Wrong configuration value: {key}={value}. '
                    '{key} should not be overridden for this broker.'.format(
                        key=k,
                        value=secondary[k],
                    ))
        options.update(secondary)
        return options

    def __get_configuration(self, app):
        configuration = self.__merge_options(
            self.__get_primary_options(),
            self.__get_secondary_options(app),
        )
        class_name = configuration.get('class', 'RabbitmqBroker')
        try:
            broker_class = configuration['class'] = _broker_classes[class_name]
        except KeyError:
            raise ValueError(
                'invalid broker class: {config_prefix}_CLASS={class_name}'.format(
                    config_prefix=self.__config_prefix,
                    class_name=class_name,
                ))
        if broker_class.__broker_default_url is not None:
            configuration.setdefault('url', broker_class.__broker_default_url)
        return configuration


class LazyActor(_ProxiedInstanceMixin, dramatiq.Actor):
    def __init__(self, fn, *, broker, **kw):
        object.__setattr__(self, '_proxied_instance', None)
        self.__fn = fn
        self.__kw = kw
        actors = getattr(broker, '_unregistered_lazy_actors', None)
        if actors is None:
            self._register_proxied_instance(broker)
        else:
            actors.append(self)

    def __call__(self, *args, **kwargs):
        if self._proxied_instance:
            return self._proxied_instance(*args, **kwargs)
        return self.__fn(*args, **kwargs)

    def _register_proxied_instance(self, broker):
        self._proxied_instance = dramatiq.Actor(self.__fn, broker=broker, **self.__kw)


class AppContextMiddleware(dramatiq.Middleware):
    state = threading.local()

    def __init__(self, app):
        self.app = app

    def before_process_message(self, broker, message):
        context = self.app.app_context()
        context.push()
        self.state.context = context

    def after_process_message(self, broker, message, *, result=None, exception=None):
        try:
            context = self.state.context
            context.pop(exception)
            del self.state.context
        except AttributeError:
            pass

    after_skip_message = after_process_message


class MultipleAppsWarningMiddleware(dramatiq.Middleware):
    def after_process_boot(self, broker):
        broker.logger.warning(
            "%s is used by more than one flask application. "
            "Actor's application context may be set incorrectly." % broker
        )


RabbitmqBroker = _create_broker_class(
    module_name='dramatiq.brokers.rabbitmq',
    class_name='RabbitmqBroker',
    default_url='amqp://127.0.0.1:5672',
)


RedisBroker = _create_broker_class(
    module_name='dramatiq.brokers.redis',
    class_name='RedisBroker',
    default_url='redis://127.0.0.1:6379/0',
)


StubBroker = _create_broker_class(
    module_name='dramatiq.brokers.stub',
    class_name='StubBroker',
    default_url=None,
)


class Broker(_LazyBrokerMixin, AbstractBroker):
    pass
