import logging
import threading
import dramatiq
import dramatiq.broker
import dramatiq.brokers.stub

_registered_config_prefixes = set()
_broker_classes_registry = {}

DEFAULT_CLASS_NAME = 'RabbitmqBroker'
DEFAULT_CONFIG_PREFIX = 'DRAMATIQ_BROKER'


def register_broker_class(broker_class):
    class_name = broker_class.__name__
    assert issubclass(broker_class, dramatiq.broker.Broker)
    assert issubclass(broker_class, LazyBrokerMixin)
    assert class_name != 'Broker'
    assert class_name not in _broker_classes_registry
    _broker_classes_registry[class_name] = broker_class


class ProxiedInstanceMixin:
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


class LazyBrokerMixin(ProxiedInstanceMixin):
    """Turns a regular dramatiq broker class into a lazy broker class.

    The derived lazy broker class should be registered with the
    `register_broker_class` function. The derived lazy broker class
    should define the following additional attributes:

    * `_dramatiq_broker_factory`: a callable that returns instances of
      the regular dramatiq broker class.

    * `_dramatiq_broker_default_url`: a string that defines the
      default broker URL, or `None` if there is none.

    The class `Broker` is the only exception to this rule. It
    represents a broker of dynamically configurable type, and can not
    be registered with `register_broker_class`.

    """

    _dramatiq_broker_default_url = None

    def __init__(self, app=None, config_prefix=DEFAULT_CONFIG_PREFIX, **options):
        object.__setattr__(self, '_proxied_instance', None)
        if not config_prefix.isupper():
            raise ValueError(
                'Invalid configuration prefix: "{}". Configuration prefixes '
                'should be all uppercase.'.format(config_prefix)
            )
        if config_prefix in _registered_config_prefixes:
            raise RuntimeError(
                'Can not create a second broker with configuration prefix "{}". '
                'Did you forget to pass the "config_prefix" argument when '
                'creating the broker?'.format(config_prefix)
            )
        _registered_config_prefixes.add(config_prefix)
        self.__app = app
        self.__config_prefix = config_prefix
        self.__options = options
        self.__configuration = None

        # When an actor is defined, broker's `actor_options` attribute
        # is accessed, which asks the middleware what actor options
        # are valid. We will delegate this work to a stub broker,
        # until our broker is ready.
        self.__stub = dramatiq.brokers.stub.StubBroker(middleware=options.get('middleware'))

        self._unregistered_lazy_actors = []
        if config_prefix == DEFAULT_CONFIG_PREFIX:
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
            broker = self._dramatiq_broker_factory(**options)
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
        options = self.__options.copy()
        options.pop('class', None)
        class_name = type(self).__name__
        if class_name in _broker_classes_registry:
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
        pclass = primary.get('class')
        sclass = secondary.get('class')
        is_class_overridden = (
            pclass != sclass
            and pclass is not None
            and sclass is not None
        )
        if is_class_overridden:
            # When the broker class is overridden, all primary options
            # other than "class" and "middleware" are irrelevant.
            options = {k: v for k, v in primary.items() if k in ['class', 'middleware']}
        else:
            options = primary.copy()
        for k, v in options.items():
            if k in secondary and v != secondary[k]:
                logging.getLogger(__name__).warning(
                    'The configuration setting "%(key)s=%(secondary_value)s" overrides '
                    'the value fixed in the source code (%(primary_value)s). This could '
                    'result in incorrect behavior.' % dict(
                        key='{}_{}'.format(self.__config_prefix, k.upper()),
                        primary_value=v,
                        secondary_value=secondary[k],
                    ))
        options.update(secondary)
        return options

    def __get_configuration(self, app):
        configuration = self.__merge_options(
            self.__get_primary_options(),
            self.__get_secondary_options(app),
        )
        class_name = configuration.get('class', DEFAULT_CLASS_NAME)
        try:
            broker_class = configuration['class'] = _broker_classes_registry[class_name]
        except KeyError:
            raise ValueError(
                'invalid broker class: {config_prefix}_CLASS={class_name}'.format(
                    config_prefix=self.__config_prefix,
                    class_name=class_name,
                ))
        if broker_class._dramatiq_broker_default_url is not None:
            configuration.setdefault('url', broker_class._dramatiq_broker_default_url)
        return configuration


class LazyActor(ProxiedInstanceMixin, dramatiq.Actor):
    """A lazily registered actor."""

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
        logging.getLogger(__name__).warning(
            "%s is used by more than one flask application. "
            "Actor's application context may be set incorrectly." % broker
        )


class Broker(LazyBrokerMixin, dramatiq.brokers.stub.StubBroker):
    """A broker of dynamically configurable type."""
