from storageprovider.providers import BaseStorageProvider


class StorageProviderClient:
    def __init__(self, provider: BaseStorageProvider):
        self.provider = provider

    def delete_object(self, container_key, object_key, system_token=None):
        self.provider.delete_object(container_key, object_key, system_token)

    def get_object_streaming(self, container_key, object_key, system_token=None):
        self.provider.get_object_streaming(container_key, object_key, system_token)

    def get_object(self, container_key, object_key, system_token=None):
        self.provider.get_object(container_key, object_key, system_token)

    def get_object_and_metadata(self, container_key, object_key, system_token=None):
        self.provider.get_object_and_metadata(container_key, object_key, system_token)

    def get_object_metadata(self, container_key, object_key, system_token=None):
        self.provider.get_object_metadata(container_key, object_key, system_token)

    def copy_object_and_create_key(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        system_token=None,
    ):
        self.provider.copy_object_and_create_key(
            source_container_key, source_object_key, output_container_key, system_token
        )

    def copy_object(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        output_object_key,
        system_token=None,
    ):
        self.provider.copy_object(
            source_container_key,
            source_object_key,
            output_container_key,
            output_object_key,
            system_token,
        )

    def update_object_and_key(self, container_key, object_data, system_token=None):
        self.provider.update_object_and_key(container_key, object_data, system_token)

    def update_object(self, container_key, object_key, object_data, system_token=None):
        self.provider.update_object(
            container_key, object_key, object_data, system_token
        )

    def list_object_keys_for_container(self, container_key, system_token=None):
        self.provider.list_object_keys_for_container(container_key, system_token)

    def get_container_data_streaming(
        self, container_key, system_token=None, translations=None
    ):
        self.provider.get_container_data_streaming(
            container_key, system_token, translations
        )

    def get_container_data(self, container_key, system_token=None, translations=None):
        self.provider.get_container_data(container_key, system_token, translations)

    def create_container(self, container_key, system_token=None):
        self.provider.create_container(container_key, system_token)

    def create_container_and_key(self, system_token=None):
        self.provider.create_container_and_key(system_token)

    def delete_container(self, container_key, system_token=None):
        self.provider.delete_container(container_key, system_token)

    def get_object_from_archive(
        self, container_key, object_key, file_name, system_token=None
    ):
        self.provider.get_object_from_archive(
            container_key, object_key, file_name, system_token
        )

    def get_object_from_archive_streaming(
        self, container_key, object_key, file_name, system_token=None
    ):
        self.provider.get_object_from_archive_streaming(
            container_key, object_key, file_name, system_token
        )

    def replace_file_in_zip_object(
        self,
        container_key,
        object_key,
        file_to_replace,
        new_file_content,
        new_file_name,
        system_token=None,
    ):
        self.provider.replace_file_in_zip_object(
            container_key,
            object_key,
            file_to_replace,
            new_file_content,
            new_file_name,
            system_token,
        )
