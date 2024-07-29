from fagfunksjoner.data.dicts import get_key_by_value


def test_flip():
    data = {"1": 2, "3": 4}
    assert get_key_by_value(data, 4) == "3"
