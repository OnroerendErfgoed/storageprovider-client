import pytest
from unittest.mock import Mock
from storageprovider.client import StorageProviderClient

test_container_key = "test_container_key"
test_object_key = "test_object_key"
test_file_name = "test.pdf"
test_object_data = b"object data"
test_new_file_name = "new_file.pdf"
test_new_file_content = Mock()


@pytest.fixture
def storage_provider_client():
    provider = Mock()
    return StorageProviderClient(provider)


def test_deletes_object(storage_provider_client):
    storage_provider_client.delete_object(test_container_key, test_object_key)
    storage_provider_client.provider.delete_object.assert_called_once_with(
        test_container_key, test_object_key, None
    )


def test_retrieves_object_streaming(storage_provider_client):
    storage_provider_client.get_object_streaming(test_container_key, test_object_key)
    storage_provider_client.provider.get_object_streaming.assert_called_once_with(
        test_container_key, test_object_key, None
    )


def test_retrieves_object(storage_provider_client):
    storage_provider_client.get_object(test_container_key, test_object_key)
    storage_provider_client.provider.get_object.assert_called_once_with(
        test_container_key, test_object_key, None
    )


def test_retrieves_object_metadata(storage_provider_client):
    storage_provider_client.get_object_metadata(test_container_key, test_object_key)
    storage_provider_client.provider.get_object_metadata.assert_called_once_with(
        test_container_key, test_object_key, None
    )


def test_copies_object_and_creates_key(storage_provider_client):
    storage_provider_client.copy_object_and_create_key(
        test_container_key, test_object_key, test_container_key
    )
    storage_provider_client.provider.copy_object_and_create_key.assert_called_once_with(
        test_container_key, test_object_key, test_container_key, None
    )


def test_updates_object_and_creates_key(storage_provider_client):
    storage_provider_client.update_object_and_key(test_container_key, test_object_data)
    storage_provider_client.provider.update_object_and_key.assert_called_once_with(
        test_container_key, test_object_data, None
    )


def test_updates_object(storage_provider_client):
    storage_provider_client.update_object(
        test_container_key, test_object_key, test_object_data
    )
    storage_provider_client.provider.update_object.assert_called_once_with(
        test_container_key, test_object_key, test_object_data, None
    )


def test_lists_object_keys_for_container(storage_provider_client):
    storage_provider_client.list_object_keys_for_container(test_container_key)
    storage_provider_client.provider.list_object_keys_for_container.assert_called_once_with(
        test_container_key, None
    )


def test_creates_container(storage_provider_client):
    storage_provider_client.create_container(test_container_key)
    storage_provider_client.provider.create_container.assert_called_once_with(
        test_container_key, None
    )


def test_deletes_container(storage_provider_client):
    storage_provider_client.delete_container(test_container_key)
    storage_provider_client.provider.delete_container.assert_called_once_with(
        test_container_key, None
    )


def test_retrieves_object_from_archive(storage_provider_client):
    storage_provider_client.get_object_from_archive(
        test_container_key, test_object_key, test_file_name
    )
    storage_provider_client.provider.get_object_from_archive.assert_called_once_with(
        test_container_key, test_object_key, test_file_name, None
    )


def test_replaces_file_in_zip_object(storage_provider_client):
    storage_provider_client.replace_file_in_zip_object(
        test_container_key,
        test_object_key,
        test_file_name,
        test_new_file_content,
        test_new_file_name,
    )
    storage_provider_client.provider.replace_file_in_zip_object.assert_called_once_with(
        test_container_key,
        test_object_key,
        test_file_name,
        test_new_file_content,
        test_new_file_name,
        None,
    )