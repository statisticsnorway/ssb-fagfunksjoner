from fagfunksjoner.api import dapla_api


class MockResponse:
    def __init__(self, json_data):
        """Initialize the response with JSON data and status-check tracking."""
        self.json_data = json_data
        self.raise_for_status_called = False

    def raise_for_status(self):
        self.raise_for_status_called = True

    def json(self):
        return self.json_data


def test_unwrap_nodes_replaces_nodes_wrappers():
    value = {
        "data": {
            "teams": {
                "nodes": [
                    {"name": "team-a", "members": {"nodes": [{"name": "Ola"}]}},
                    {"name": "team-b", "members": {"nodes": []}},
                ]
            }
        }
    }

    assert dapla_api._unwrap_nodes(value) == {
        "data": {
            "teams": [
                {"name": "team-a", "members": [{"name": "Ola"}]},
                {"name": "team-b", "members": []},
            ]
        }
    }


def test_unwrap_nodes_leaves_scalars_and_non_wrapper_dicts():
    value = {
        "nodes": ["kept because dict has another key"],
        "totalCount": 1,
        "active": True,
        "empty": None,
    }

    assert dapla_api._unwrap_nodes(value) == value


def test_get_from_dapla_api_posts_query_with_token(monkeypatch):
    query = "query { teams { nodes { name } } }"
    response = MockResponse(
        {"data": {"teams": {"nodes": [{"name": "team-a"}]}}},
    )
    post_calls = []

    def mock_fetch_personal_token(audiences):
        assert audiences == ["dapla-api"]
        return "test-token"

    def mock_post(url, headers, json):
        post_calls.append({"url": url, "headers": headers, "json": json})
        return response

    monkeypatch.setattr(
        dapla_api.AuthClient,
        "fetch_personal_token",
        mock_fetch_personal_token,
    )
    monkeypatch.setattr(dapla_api.requests, "post", mock_post)

    result = dapla_api.get_from_dapla_api(query)

    assert result == {"data": {"teams": [{"name": "team-a"}]}}
    assert response.raise_for_status_called is True
    assert post_calls == [
        {
            "url": dapla_api.DAPLA_CTRL_API_URI,
            "headers": {"Authorization": "Bearer test-token"},
            "json": {"query": query},
        }
    ]


def test_get_from_dapla_api_can_return_raw_response(monkeypatch):
    raw_response = {"data": {"teams": {"nodes": [{"name": "team-a"}]}}}
    response = MockResponse(raw_response)

    monkeypatch.setattr(
        dapla_api.AuthClient,
        "fetch_personal_token",
        lambda audiences: "test-token",
    )
    monkeypatch.setattr(dapla_api.requests, "post", lambda *args, **kwargs: response)

    assert dapla_api.get_from_dapla_api("query", unwrap_nodes=False) == raw_response
