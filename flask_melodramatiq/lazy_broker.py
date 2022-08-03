import logging
import threading
import dramatiq
import dramatiq.brokers.stub

_registered_config_prefixes = set()
_broker_classes_registry = {}


class missing:
    """Missing value for configuration variables. This can be useful when
    you want to document the given configuration setting in your code,
    but you do not want to change the default value.

    For example::

        from flask import Flask
        from flask_melodramatiq import missing

        app = Flask(__name__)
        app.config['DRAMATIQ_BROKER_URL'] = missing
    """


DEFAULT_CLASS_NAME = 'RabbitmqBroker'
DEFAULT_CONFIG_PREFIX = 'DRAMATIQ_BROKER'
LAZY_BROKER_DOCSTRING_TEMPLATE = """{description}
    :param app: An optonal Flask application instance

    :param config_prefix: A prefix for the Flask configuration
       settings for this broker instance. Each broker instance should
       have a unique configuration settings prefix.

    :param options: Keyword arguments to be passed to the constructor
       of the wrapped `dramatiq` broker class.
"""


def register_broker_class(broker_class):
    class_name = broker_class.__name__
    assert issubclass(broker_class, dramatiq.Broker)
    assert issubclass(broker_class, LazyBrokerMixin)
    if class_name == 'Broker' or class_name in _broker_classes_registry:
        raise RuntimeError('"{}" is already registered.'.format(class_name))
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
        if self._proxied_instance is None:
            return object.__repr__(self)
        return repr(self._proxied_instance)

    def __getattr__(self, name):
        if self._proxied_instance is None:
            raise RuntimeError(
                'init_app() must be called on lazy brokers before use. '
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
    should define the following additional attribute:

    * `_dramatiq_broker_factory`: a callable that returns instances of
      the regular dramatiq broker class.

    The class `Broker` is the only exception to this rule. It
    represents a broker of dynamically configurable type, and can not
    be registered with `register_broker_class`.

    """

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
        self.__orig_class_name = type(self).__name__

        # We want to be able to add middleware and declare actors
        # before `init_app` is called. We do this by delegating to a
        # stub broker, until our broker is ready.
        self.__stub = dramatiq.brokers.stub.StubBroker(middleware=options.pop('middleware', None))

        # We want to be be capabale of registering actors that might store
        # results. In that end, we add a stub backend results proxy.
        self.__empty_backend = dramatiq.results.Results()
        self.__stub.add_middleware(self.__empty_backend)

        self._unregistered_lazy_actors = []
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """This method can be used to initialize an application for the use
        with this broker. A broker can not be used in the context of
        an application unless it is initialized that way.

        :meth:`init_app` is called automatically if an ``app``
        argument is passed to the constructor.

        """

        if self.__stub:
            self.__options['middleware'] = [
                m for m in self.__stub.middleware
                if m is not self.__empty_backend
            ]
            configuration = self.__get_configuration(app)
            self.__stub.close()
            self.__stub = None
            self.__app = app
            self.__configuration = configuration
            options = configuration.copy()
            self.__class__ = options.pop('class')

            # Instanciate dramatiq Broker
            broker = self._dramatiq_broker_factory(**options)

            # Add Flask App Context Middleware
            broker.add_middleware(AppContextMiddleware(app))

            # Register actors on broker
            for actor in self._unregistered_lazy_actors:
                actor._register_proxied_instance(broker=broker)

            self._unregistered_lazy_actors = None
            self._proxied_instance = broker  # `self` is sealed from now on.
        else:
            configuration = self.__get_configuration(app)
        if configuration != self.__configuration:
            raise RuntimeError(
                '{} tried to reconfigure an already configured broker.'.format(app)
            )
        if app is not self.__app:
            self._proxied_instance.add_middleware(MultipleAppsWarningMiddleware())
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions[self.__config_prefix.lower()] = self

    def set_default(self):
        """Configure this broker instance to be the global broker instance.

        Calls :func:`dramatiq.set_broker` internally.
        """

        dramatiq.set_broker(self)

    def actor(self, fn=None, **kw):
        """Declare an actor for this broker instance.

        Calls :func:`dramatiq.actor` internally.

        Example::

          from flask_melodramatiq import Broker

          broker = Broker()

          @broker.actor
          def task():
              print('Snakes appreciate good theatrical preformace.')
        """

        for kwarg in ['broker', 'actor_class']:
            if kwarg in kw:
                raise TypeError(
                    '{class_name}.actor() got an unexpected keyword argument "{kwarg}".'.format(
                        class_name=type(self).__name__,
                        kwarg=kwarg,
                    ))
        decorator = dramatiq.actor(actor_class=LazyActor, broker=self, **kw)
        if fn is None:
            return decorator
        return decorator(fn)

    @property
    def actor_options(self):
        return (self._proxied_instance or self.__stub).actor_options

    def add_middleware(self, middleware, *, before=None, after=None):
        return (self._proxied_instance or self.__stub).add_middleware(middleware, before=before, after=after)

    def __get_primary_options(self):
        options = self.__options.copy()
        options.pop('class', None)
        class_name = self.__orig_class_name
        if class_name in _broker_classes_registry:
            options['class'] = class_name
        return options

    def __get_secondary_options(self, app):
        prefix = '{}_'.format(self.__config_prefix)
        options = {
            k[len(prefix):].lower(): v
            for k, v in app.config.items()
            if k.isupper() and k.startswith(prefix) and v is not missing
        }
        if 'middleware' in options:
            value = options.pop('middleware')
            logging.getLogger(__name__).warning(
                'Ignored configuration setting: "%(key)s=%(value)s". '
                'Broker middleware can not be altered in app configuration.',
                dict(
                    key='{}_MIDDLEWARE'.format(self.__config_prefix),
                    value=value,
                ),
            )
        return options

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
                    'result in incorrect behavior.',
                    dict(
                        key='{}_{}'.format(self.__config_prefix, k.upper()),
                        primary_value=v,
                        secondary_value=secondary[k],
                    ),
                )
        options.update(secondary)
        return options

    def __get_configuration(self, app):
        configuration = self.__merge_options(
            self.__get_primary_options(),
            self.__get_secondary_options(app),
        )
        class_name = configuration.get('class', DEFAULT_CLASS_NAME)
        try:
            configuration['class'] = _broker_classes_registry[class_name]
        except KeyError:
            raise ValueError(
                'Invalid broker class: "{config_prefix}_CLASS={class_name}".'.format(
                    config_prefix=self.__config_prefix,
                    class_name=class_name,
                ))
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
        if self._proxied_instance is None:
            return self.__fn(*args, **kwargs)
        return self._proxied_instance(*args, **kwargs)

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
            "%(broker)s is used by more than one flask application. "
            "Actor's application context may be set incorrectly.",
            dict(
                broker=broker
            ),
        )


class Broker(LazyBrokerMixin, dramatiq.brokers.stub.StubBroker):
    __doc__ = LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description="""A lazy broker of dynamically configurable type.

    The type of the broker should be specified by the
    "*config_prefix*\_CLASS" setting in the Flask application
    configuration. For example, if *config_prefix* is the defaut one,
    the configuration setting: ``DRAMATIQ_BROKER_CLASS="RedisBroker"``
    specifies that the type of the broker should be
    :class:`~RedisBroker`.

    The following broker type names are always valid:

    * ``"RabbitmqBroker"`` (default)
    * ``"RedisBroker"``
    * ``"StubBroker"``

    In addition to these, custom broker types can be registered with
    :func:`~flask_melodramatiq.create_broker_class`.

    """,
    )  # noqa: W291
