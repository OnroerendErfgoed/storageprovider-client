import re

from storageprovider.providers import BaseStorageProvider

from minio import Minio


class MinioProvider(BaseStorageProvider):
    def __init__(self, server_url, access_key, secret_key, bucket_name):
        self.bucket_name = bucket_name
        self.client = Minio(
            server_url, access_key=access_key, secret_key=secret_key, secure=False
        )

    def _clean_identifier(identifier: str) -> str:
        """
        https://datatracker.ietf.org/doc/html/draft-kunze-pairtree-01#section-3
        """
        chars_to_hex = '"*+,<=>?\^|'
        chars_conversion = {"/": "=", ":": "+", ".": ","}
        cleaned = []

        for c in identifier:
            c_dec = ord(c)
            if c_dec >= 33 and c_dec <= 126 and c not in chars_to_hex:
                c_lower = c.lower()
                cleaned.append(c_lower)
            elif c in chars_to_hex:
                cleaned.append("^" + format(ord(c), "x"))

        converted = [chars_conversion.get(char, char) for char in cleaned]
        return "".join(converted)

    def _id_to_pairtree_path(identifier: str) -> str:
        """
        Converts a cleaned identifier to a PairTree path using 2-character segments.
        """
        cleaned = clean_identifier(identifier)
        segments = [cleaned[i : i + 2] for i in range(0, len(cleaned), 2)]
        return "/".join(segments) + "/"

    def delete_object(self, container_key, object_key, system_token=None):
        """
        delete an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        client.remove_object(
            self.bucket_name, f"{self._id_to_pairtree_path(container_key)}{object_key}"
        )

    def get_object_streaming(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store as a stream
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        try:
            response = client.get_object(
                self.bucket_name,
                f"{self._id_to_pairtree_path(container_key)}{object_key}",
            )
            return response.stream(1024 * 1024)
        finally:
            response.close()
            response.release_conn()

    def get_object(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        try:
            response = client.get_object(
                self.bucket_name,
                f"{self._id_to_pairtree_path(container_key)}{object_key}",
            )
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def get_object_and_metadata(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store and also return header meta data

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        try:
            response = client.get_object(
                self.bucket_name,
                f"{self._id_to_pairtree_path(container_key)}{object_key}",
            )
            result = client.stat_object(
                self.bucket_name,
                f"{self._id_to_pairtree_path(container_key)}{object_key}",
            )
            metadata["mime"] = result.content_type
            metadata["size"] = result.size
            return {"object": response.read(), "metadata": metadata}
        finally:
            response.close()
            response.release_conn()

    def get_object_metadata(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return headers of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        result = client.stat_object(
            self.bucket_name, f"{self._id_to_pairtree_path(container_key)}{object_key}"
        )
        metadata["mime"] = result.content_type
        metadata["size"] = result.size
        return metadata

    def copy_object_and_create_key(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        system_token=None,
    ):
        """
        Copy an object and create key in the data store

        :param source_container_key: key of the source container in the data store
        :param source_object_key: key of the source object in the container
        :param output_container_key: key of output container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        output_object_key = str(uuid.uuid4())
        client.copy_object(
            self.bucket_name,
            f"{self._id_to_pairtree_path(output_container_key)}{output_object_key}",
            CopySource(
                self.bucket_name,
                f"{self._id_to_pairtree_path(source_container_key)}{source_object_key}",
            ),
        )
        return output_object_key

    def copy_object(
        self,
        source_container_key,
        source_object_key,
        output_container_key,
        output_object_key,
        system_token=None,
    ):
        """
        Copy an object in the data store to specific key

        :param source_container_key: key of the source container in the data store
        :param source_object_key: key of the source object in the container
        :param output_container_key: key of output container in the data store
        :param output_object_key: specific object key for the output object in the container
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        client.copy_object(
            self.bucket_name,
            f"{self._id_to_pairtree_path(output_container_key)}{output_object_key}",
            CopySource(
                self.bucket_name,
                f"{self._id_to_pairtree_path(source_container_key)}{source_object_key}",
            ),
        )

    def update_object_and_key(self, container_key, object_data, system_token=None):
        """
        create an object and key in the data store

         :param container_key: key of the container in the data store
         :param object_data: data of the object
         :param system_token: oauth system token
         :raises InvalidStateException: if the response is in an invalid state
        """
        object_key = str(uuid.uuid4())
        client.put_object(
            self.bucket_name,
            f"{self._id_to_pairtree_path(container_key)}{object_key}",
            object_data,
        )
        return object_key

    def update_object(self, container_key, object_key, object_data, system_token=None):
        """
        update (or create) an object in the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param object_data: data of the object
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        client.put_object(
            self.bucket_name,
            f"{self._id_to_pairtree_path(container_key)}{object_key}",
            object_data,
        )

    def list_object_keys_for_container(self, container_key, system_token=None):
        """
        list all object keys for a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :return list of object keys found in the container
        :raises InvalidStateException: if the response is in an invalid state
        """
        objects = client.list_objects(
            self.bucket_name, prefix=f"{self._id_to_pairtree_path(container_key)}"
        )
        return objects

    def get_container_data_streaming(
        self, container_key, system_token=None, translations=None
    ):
        """
        Retrieve a zip of a container in the data store as a stream
        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :param translations: Dict of object IDs and file names to use for them.
        :return zip of objects as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        raise NotImplementedError(
            "get_container_data_streaming is not implemented for MinioProvider"
        )

    def get_container_data(self, container_key, system_token=None, translations=None):
        """
        Retrieve a zip of a container in the data store.

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :param translations: Dict of object IDs and file names to use for them.
        :return list of object keys found in the container
        :raises InvalidStateException: if the response is in an invalid state
        """
        translations = translations or {}

        def fetch_object(object_key):
            """
            Fetches the content of a single object.
            :param object_key: Key of the object to fetch
            :return: Tuple of object key and content
            """
            response = client.get_object(bucket_name, object_key)
            try:
                return object_key, response.read()
            finally:
                response.close()
                response.release_conn()

        # List all objects under the container key
        objects = client.list_objects(bucket_name, prefix=f"{container_key}")

        # Fetch all objects concurrently
        object_contents = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_object = {
                executor.submit(fetch_object, obj.object_name): obj.object_name
                for obj in objects
            }
            for future in concurrent.futures.as_completed(future_to_object):
                object_key = future_to_object[future]
                try:
                    object_contents.append(future.result())
                except Exception as e:
                    print(f"Error fetching object {object_key}: {e}")

        # Create a zip file containing all the objects
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            for object_key, content in object_contents:
                zip_file.writestr(translations.get(object_key, object_key), content)

        zip_buffer.seek(0)
        return zip_buffer

    def create_container(self, container_key, system_token=None):
        """
        create a new container with specific key in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        raise NotImplementedError(
            "create_container is not implemented for MinioProvider"
        )

    def create_container_and_key(self, system_token=None):
        """
        create a new container in the data store and generate key

        :param system_token: oauth system token
        :return the key generated for the container
        :raises InvalidStateException: if the response is in an invalid state
        """
        raise NotImplementedError(
            "create_container_and_key is not implemented for MinioProvider"
        )

    def delete_container(self, container_key, system_token=None):
        """
        delete a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        client.remove_object(
            self.bucket_name, f"{self._id_to_pairtree_path(container_key)}"
        )

    def get_object_from_archive(
        self, container_key, object_key, file_name, system_token=None
    ):
        """
        retrieve an object from an archive in the data store
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param file_name: name of the file to get from the zip
        :param system_token: oauth system token
        :return content of the object as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        raise NotImplementedError(
            "get_object_from_archive is not implemented for MinioProvider"
        )

    def get_object_from_archive_streaming(
        self, container_key, object_key, file_name, system_token=None
    ):
        """
        retrieve an object from an archive in the data storeas a stream
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        raise NotImplementedError(
            "get_object_from_archive_streaming is not implemented for MinioProvider"
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
        """
        replace a file in a zip in the data store
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param file_to_replace: name of the file to replace in the zip
        :param new_file_content: content of the new file
        :param new_file_name: name of the new file
        :param system_token: oauth system token
        :return content of the updated zip file
        """
        raise NotImplementedError(
            "replace_file_in_zip_object is not implemented for MinioProvider"
        )
