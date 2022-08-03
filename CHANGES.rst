Changelog
=========

Version 1.0
-----------

- Removed the `RabbitmqBroker.publish_message()` method.

- Documented the use of `missing` configuration values.


Version 0.3.9
-------------

- Fixed a bug. The bug resulted in raising "tried to reconfigure an
  already configured broker" error in case at least one configuration
  option has been passed to the borker constructor, and the broker's
  type is changed in the flask's configuration.


Version 0.3.8
-------------

- Better support for `dramatiq.results.Results`


Version 0.3.7
-------------

- Removed support for `Pika` < 1.0.
- Fixd a connection timeout problem when using
  `RabbitmqBroker.publish_message()`


Version 0.3.6
-------------

- Allow `add_middleware` to be called on brokers before `init_app`.


Version 0.3.5
-------------

- Added `routing_key` argument to the
  `RabbitmqBroker.publish_message()` method.
- Fixed minor documentation issue


Version 0.3.4
-------------

- Added `RabbitmqBroker.publish_message()` method.
- Added `.circleci` directory


Version 0.3.3
-------------

- Fixed minor setuptools packaging issues.


Version 0.3.2
-------------

- Fixed minor setuptools packaging issues.


Version 0.3.1
-------------

- Added public `create_broker_class` function.
- Improved documentation


Version 0.3
-----------

- Initial public release
