from src.dummy import dummy


class TestDummy:
    def test_dummy_does_nothing(self):
        assert dummy() is None
