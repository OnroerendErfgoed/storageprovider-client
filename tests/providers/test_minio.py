import pytest
import unittest
from unittest.mock import MagicMock
from storageprovider.providers.minio import MinioProvider


@pytest.fixture
def minio_provider():
    server_url = "localhost:9000"
    access_key = "test-access-key"
    secret_key = "test-secret-key"
    bucket_name = "test-bucket"
    provider = MinioProvider(server_url, access_key, secret_key, bucket_name)
    provider.client = MagicMock()
    return provider


def test_clean_identifier(minio_provider):
    identifier = "test:/identifier"
    expected = "test+=identifier"
    result = minio_provider._clean_identifier(identifier)
    assert result == expected


def test_id_to_pairtree_path(minio_provider):
    identifier = "test:/identifier"
    expected = "te/st/+=/id/en/ti/fi/er/"
    result = minio_provider._id_to_pairtree_path(identifier)
    assert result == expected


def test_delete_object(minio_provider):
    container_key = "container"
    object_key = "object"
    minio_provider.delete_object(container_key, object_key)
    minio_provider.client.remove_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )


def test_get_object_streaming(minio_provider):
    container_key = "container"
    object_key = "object"
    mock_response = MagicMock()
    mock_response.stream.return_value = [b"chunk1", b"chunk2"]
    minio_provider.client.get_object.return_value = mock_response

    chunks = list(minio_provider.get_object_streaming(container_key, object_key))

    assert chunks == [b"chunk1", b"chunk2"]
    minio_provider.client.get_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_get_object(minio_provider):
    container_key = "container"
    object_key = "object"
    mock_response = MagicMock()
    mock_response.read.return_value = b"data"
    minio_provider.client.get_object.return_value = mock_response

    result = minio_provider.get_object(container_key, object_key)

    assert result == b"data"
    minio_provider.client.get_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_get_object_and_metadata(minio_provider):
    container_key = "container"
    object_key = "object"
    mock_response = MagicMock()
    mock_response.read.return_value = b"data"
    mock_stat = MagicMock()
    mock_stat.last_modified = "2023-01-01T00:00:00Z"
    mock_stat.content_type = "application/json"
    mock_stat.size = 1234
    minio_provider.client.get_object.return_value = mock_response
    minio_provider.client.stat_object.return_value = mock_stat

    result = minio_provider.get_object_and_metadata(container_key, object_key)

    assert result == {
        "object": b"data",
        "metadata": {
            "time_last_modification": "2023-01-01T00:00:00Z",
            "mime": "application/json",
            "size": 1234,
        },
    }
    minio_provider.client.get_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )
    minio_provider.client.stat_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_get_object_metadata(minio_provider):
    container_key = "container"
    object_key = "object"
    mock_stat = MagicMock()
    mock_stat.last_modified = "2023-01-01T00:00:00Z"
    mock_stat.content_type = "application/json"
    mock_stat.size = 1234
    minio_provider.client.stat_object.return_value = mock_stat

    result = minio_provider.get_object_metadata(container_key, object_key)

    assert result == {
        "time_last_modification": "2023-01-01T00:00:00Z",
        "mime": "application/json",
        "size": 1234,
    }
    minio_provider.client.stat_object.assert_called_once_with(
        minio_provider.bucket_name, "co/nt/ai/ne/r/object"
    )


def test_copy_object_and_create_key(minio_provider):
    source_container_key = "source_container"
    source_object_key = "source_object"
    output_container_key = "output_container"
    mock_uuid = "1234-5678-uuid"
    with unittest.mock.patch("uuid.uuid4", return_value=mock_uuid):
        result = minio_provider.copy_object_and_create_key(
            source_container_key, source_object_key, output_container_key
        )
    assert result == mock_uuid
    minio_provider.client.copy_object.assert_called_once()


def test_copy_object(minio_provider):
    source_container_key = "source_container"
    source_object_key = "source_object"
    output_container_key = "output_container"
    output_object_key = "output_object"
    minio_provider.copy_object(
        source_container_key, source_object_key, output_container_key, output_object_key
    )
    minio_provider.client.copy_object.assert_called_once()


def test_update_object_and_key(minio_provider):
    container_key = "container"
    object_data = b"data"
    mock_uuid = "1234-5678-uuid"
    with unittest.mock.patch("uuid.uuid4", return_value=mock_uuid):
        result = minio_provider.update_object_and_key(container_key, object_data)
    assert result == mock_uuid
    minio_provider.client.put_object.assert_called_once()


def test_update_object(minio_provider):
    container_key = "container"
    object_key = "object"
    object_data = b"data"
    minio_provider.update_object(container_key, object_key, object_data)
    minio_provider.client.put_object.assert_called_once()


def test_list_object_keys_for_container(minio_provider):
    container_key = "container"
    mock_object = MagicMock()
    mock_object.object_name = "co/nt/ai/ne/r/object"
    minio_provider.client.list_objects.return_value = [mock_object]

    result = list(minio_provider.list_object_keys_for_container(container_key))

    assert result == [mock_object]
    minio_provider.client.list_objects.assert_called_once()


def test_get_container_data_streaming_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.get_container_data_streaming("container")


def test_get_container_data(minio_provider):
    container_key = "container"
    mock_object = MagicMock()
    mock_object.object_name = "co/nt/ai/ne/r/object"
    minio_provider.client.list_objects.return_value = [mock_object]
    mock_response = MagicMock()
    mock_response.read.return_value = b"data"
    minio_provider.client.get_object.return_value = mock_response

    result = minio_provider.get_container_data(container_key)

    assert isinstance(result, bytes)
    minio_provider.client.list_objects.assert_called_once()
    minio_provider.client.get_object.assert_called_once()


def test_create_container_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.create_container("container")


def test_create_container_and_key_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.create_container_and_key()


def test_delete_container(minio_provider):
    container_key = "container"
    minio_provider.delete_container(container_key)
    minio_provider.client.list_objects.assert_called_once()


def test_get_object_from_archive_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.get_object_from_archive("container", "object", "file_name")


def test_get_object_from_archive_streaming_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.get_object_from_archive_streaming(
            "container", "object", "file_name"
        )


def test_replace_file_in_zip_object_not_implemented(minio_provider):
    with pytest.raises(NotImplementedError):
        minio_provider.replace_file_in_zip_object(
            "container", "object", "file_to_replace", b"new_content", "new_file_name"
        )
