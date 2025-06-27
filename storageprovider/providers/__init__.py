from abc import ABC, abstractmethod


class BaseStorageProvider(ABC):
    @abstractmethod
    def delete_object(self, container_key, object_key, system_token=None):
        pass

    @abstractmethod
    def get_object(self, container_key, object_key, system_token=None):
        pass

    @abstractmethod
    def get_object_streaming(self, container_key, object_key, system_token=None):
        pass

    @abstractmethod
    def get_object_and_metadata(self, container_key, object_key, system_token=None):
        pass

    @abstractmethod
    def get_object_metadata(self, container_key, object_key, system_token=None):
        pass

    @abstractmethod
    def copy_object_and_create_key(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        system_token=None,
    ):
        pass

    @abstractmethod
    def copy_object(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        output_object_key,
        system_token=None,
    ):
        pass

    @abstractmethod
    def update_object_and_key(self, container_key, object_data, system_token=None):
        pass

    @abstractmethod
    def update_object(self, container_key, object_key, object_data, system_token=None):
        pass

    @abstractmethod
    def list_object_keys_for_container(self, container_key, system_token=None):
        pass

    @abstractmethod
    def get_container_data_streaming(
        self, container_key, system_token=None, translations=None
    ):
        pass

    @abstractmethod
    def get_container_data(self, container_key, system_token=None, translations=None):
        pass

    @abstractmethod
    def create_container(self, container_key, system_token=None):
        pass

    @abstractmethod
    def create_container_and_key(self, system_token=None):
        pass

    @abstractmethod
    def delete_container(self, container_key, system_token=None):
        pass

    @abstractmethod
    def get_object_from_archive(
        self, container_key, object_key, file_name, system_token=None
    ):
        pass

    @abstractmethod
    def get_object_from_archive_streaming(
        self, container_key, object_key, file_name, system_token=None
    ):
        pass

    @abstractmethod
    def replace_file_in_zip_object(
        self,
        container_key,
        object_key,
        file_to_replace,
        new_file_content,
        new_file_name,
        system_token=None,
    ):
        pass
