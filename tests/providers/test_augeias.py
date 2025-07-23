import pytest
from unittest.mock import patch, MagicMock
from storageprovider.providers.augeias import AugeiasProvider, InvalidStateException


@pytest.fixture
def augeias_provider():
    base_url = "http://localhost:8000"
    collection = "test-collection"
    return AugeiasProvider(base_url, collection)


@patch("storageprovider.providers.augeias.requests")
def test_deletes_object_successfully(mock_requests, augeias_provider):
    mock_requests.delete.return_value.status_code = 200
    response = augeias_provider.delete_object("container", "object")
    mock_requests.delete.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object",
        headers={},
    )
    assert response.status_code == 200


@patch("storageprovider.providers.augeias.requests")
def test_raises_exception_when_deleting_nonexistent_object(mock_requests, augeias_provider):
    mock_requests.delete.return_value.status_code = 404
    mock_requests.delete.return_value.text = "Object not found"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.delete_object("container", "nonexistent_object")
    assert context.value.status_code == 404
    assert str(context.value) == "Object not found, http status code: 404"


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_object_streaming_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.iter_content.return_value = [b"chunk1", b"chunk2"]
    result = list(augeias_provider.get_object_streaming("container", "object"))
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object",
        headers={},
        stream=True,
    )
    assert result == [b"chunk1", b"chunk2"]


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_object_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.content = b"object content"
    result = augeias_provider.get_object("container", "object")
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object",
        headers={},
    )
    assert result == b"object content"


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_object_and_metadata_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.content = b"object content"
    mock_requests.get.return_value.headers = {
        "Content-Type": "application/json",
        "Content-Length": "1234",
    }
    result = augeias_provider.get_object_and_metadata("container", "object")
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object",
        headers={},
    )
    assert result == {
        "object": b"object content",
        "metadata": {
            "Content-Type": "application/json",
            "Content-Length": "1234",
            "mime": "application/json",
            "size": "1234",
        },
    }


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_object_metadata_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.json.return_value = {
        "mime": "application/json",
        "size": 1234,
    }
    result = augeias_provider.get_object_metadata("container", "object")
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object/meta",
        headers={},
    )
    assert result == {
        "mime": "application/json",
        "size": 1234,
        "Content-Type": "application/json",
        "Content-Length": 1234,
    }


@patch("storageprovider.providers.augeias.requests")
def test_copies_object_and_creates_key_successfully(mock_requests, augeias_provider):
    mock_requests.post.return_value.status_code = 201
    mock_requests.post.return_value.json.return_value = {"object_key": "new_object_key"}
    result = augeias_provider.copy_object_and_create_key(
        "source_container", "source_object", "output_container"
    )
    mock_requests.post.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/output_container",
        headers={"content-type": "application/json"},
        json={
            "host_url": "http://localhost:8000",
            "collection_key": "test-collection",
            "container_key": "source_container",
            "object_key": "source_object",
        },
    )
    assert result == "new_object_key"


@patch("storageprovider.providers.augeias.requests")
def test_copies_object_successfully(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 200
    augeias_provider.copy_object(
        "source_container", "source_object", "output_container", "output_object"
    )
    mock_requests.put.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/output_container/output_object",
        headers={"content-type": "application/json"},
        json={
            "host_url": "http://localhost:8000",
            "collection_key": "test-collection",
            "container_key": "source_container",
            "object_key": "source_object",
        },
    )


@patch("storageprovider.providers.augeias.requests")
def test_updates_object_and_creates_key_successfully(mock_requests, augeias_provider):
    mock_requests.post.return_value.status_code = 201
    mock_requests.post.return_value.json.return_value = {"object_key": "new_object_key"}
    result = augeias_provider.update_object_and_key("container", b"object data")
    mock_requests.post.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={"content-type": "application/octet-stream"},
        data=b"object data",
    )
    assert result == "new_object_key"


@patch("storageprovider.providers.augeias.requests")
def test_updates_object_successfully(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 200
    augeias_provider.update_object("container", "object", b"object data")
    mock_requests.put.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object",
        headers={"content-type": "application/octet-stream"},
        data=b"object data",
    )


@patch("storageprovider.providers.augeias.requests")
def test_lists_object_keys_for_container_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.content = b'["object1", "object2"]'
    result = augeias_provider.list_object_keys_for_container("container")
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={"Accept": "application/json"},
    )
    assert result == b'["object1", "object2"]'


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_container_data_streaming_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.iter_content.return_value = [b"chunk1", b"chunk2"]
    result = list(
        augeias_provider.get_container_data_streaming("container", translations={})
    )
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={"Accept": "application/zip"},
        stream=True,
        params={},
    )
    assert result == [b"chunk1", b"chunk2"]


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_container_data_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.content = b"zip content"
    result = augeias_provider.get_container_data("container", translations={})
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={"Accept": "application/zip"},
        params={},
    )
    assert result == b"zip content"


@patch("storageprovider.providers.augeias.requests")
def test_creates_container_successfully(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 200
    response = augeias_provider.create_container("container")
    mock_requests.put.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={},
    )
    assert response.status_code == 200


@patch("storageprovider.providers.augeias.requests")
def test_creates_container_and_key_successfully(mock_requests, augeias_provider):
    mock_requests.post.return_value.status_code = 201
    mock_requests.post.return_value.json.return_value = {"container_key": "new_container_key"}
    result = augeias_provider.create_container_and_key()
    mock_requests.post.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers",
        headers={},
    )
    assert result == "new_container_key"


@patch("storageprovider.providers.augeias.requests")
def test_deletes_container_successfully(mock_requests, augeias_provider):
    mock_requests.delete.return_value.status_code = 200
    response = augeias_provider.delete_container("container")
    mock_requests.delete.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container",
        headers={},
    )
    assert response.status_code == 200


@patch("storageprovider.providers.augeias.requests")
def test_retrieves_object_from_archive_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.content = b"file content"
    result = augeias_provider.get_object_from_archive(
        "container", "object", "file_name"
    )
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object/file_name",
        headers={},
    )
    assert result == b"file content"


@patch("storageprovider.providers.augeias.requests")
def test_streams_object_from_archive_successfully(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.iter_content.return_value = [b"chunk1", b"chunk2"]
    result = list(
        augeias_provider.get_object_from_archive_streaming(
            "container", "object", "file_name"
        )
    )
    mock_requests.get.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object/file_name",
        headers={},
        stream=True,
    )
    assert result == [b"chunk1", b"chunk2"]


@patch("storageprovider.providers.augeias.requests")
def test_replaces_file_in_zip_object_successfully(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 200
    mock_requests.put.return_value.json.return_value = {"status": "success"}
    new_file_name = "new_file.pdf"
    new_file_content = MagicMock()
    result = augeias_provider.replace_file_in_zip_object(
        "container", "object", "file_to_replace", new_file_content, new_file_name
    )
    mock_requests.put.assert_called_once_with(
        "http://localhost:8000/collections/test-collection/containers/container/object/file_to_replace",
        headers={},
        data=new_file_content,
        params={"new_file_name": new_file_name},
    )
    assert result == {"status": "success"}

@patch("storageprovider.providers.augeias.requests")
def test_fails_to_delete_object_due_to_server_error(mock_requests, augeias_provider):
    mock_requests.delete.return_value.status_code = 500
    mock_requests.delete.return_value.text = "Internal Server Error"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.delete_object("container", "object")
    assert context.value.status_code == 500
    assert str(context.value) == "Internal Server Error, http status code: 500"


@patch("storageprovider.providers.augeias.requests")
def test_fails_to_retrieve_object_due_to_not_found(mock_requests, augeias_provider):
    mock_requests.get.return_value.status_code = 404
    mock_requests.get.return_value.text = "Object not found"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.get_object("container", "nonexistent_object")
    assert context.value.status_code == 404
    assert str(context.value) == "Object not found, http status code: 404"


@patch("storageprovider.providers.augeias.requests")
def test_fails_to_copy_object_due_to_invalid_input(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 400
    mock_requests.put.return_value.text = "Bad Request"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.copy_object(
            "source_container", "source_object", "output_container", "output_object"
        )
    assert context.value.status_code == 400
    assert str(context.value) == "Bad Request, http status code: 400"


@patch("storageprovider.providers.augeias.requests")
def test_fails_to_update_object_due_to_unauthorized(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 401
    mock_requests.put.return_value.text = "Unauthorized"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.update_object("container", "object", b"object data")
    assert context.value.status_code == 401
    assert str(context.value) == "Unauthorized, http status code: 401"


@patch("storageprovider.providers.augeias.requests")
def test_fails_to_create_container_due_to_conflict(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 409
    mock_requests.put.return_value.text = "Conflict"
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.create_container("existing_container")
    assert context.value.status_code == 409
    assert str(context.value) == "Conflict, http status code: 409"


@patch("storageprovider.providers.augeias.requests")
def test_fails_to_replace_file_in_zip_due_to_invalid_file_name(mock_requests, augeias_provider):
    mock_requests.put.return_value.status_code = 400
    mock_requests.put.return_value.text = "Invalid file name"
    new_file_content = MagicMock()
    with pytest.raises(InvalidStateException) as context:
        augeias_provider.replace_file_in_zip_object(
            "container", "object", "invalid_file", new_file_content, "new_file.pdf"
        )
    assert context.value.status_code == 400
    assert str(context.value) == "Invalid file name, http status code: 400"