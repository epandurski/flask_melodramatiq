from flask_melodramatiq import StubBroker


def test_create_stub_broker():
    b = StubBroker()
    assert b
