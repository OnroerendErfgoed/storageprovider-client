import logging
from typing import Callable

import requests
from requests import RequestException
from requests import Response

LOG = logging.getLogger(__name__)


class StorageProviderClient:
    def __init__(self, base_url, collection):
        self.host_url = base_url
        self.base_url = base_url + "/collections/" + collection
        self.collection = collection

    @staticmethod
    def get_auth_header(system_token):
        return {"Authorization": f"Bearer {system_token}"}

    def _execute_requests_method(
        self,
        requests_method: Callable,
        system_token: str,
        url: str,
        response_code: int = 200,
        headers: dict = None,
        **requests_kwargs,
    ) -> Response:
        """
        Send a request with the given params.

        This is a simple utility method to handle authorization headers,
        basic accept, content-type headers and catch request exceptions.

        :param requests_method: a requests method to call. eg. requests.get, requests.post
        :param system_token: oauth system token
        :param url: url to post to.
        :param response_code: expected response code
        :param headers: extra headers to add to the request
        :param requests_kwargs: extra kwargs which will be added to the requests call.
        :return: The response
        """
        headers = headers or {}
        if system_token:
            headers.update(self.get_auth_header(system_token))
        try:
            response = requests_method(url, headers=headers, **requests_kwargs)
        except RequestException:
            LOG.exception(f"{requests_method} {url} failed.")
            raise
        if response.status_code != response_code:
            raise InvalidStateException(response.status_code, response.text)

        return response

    def delete_object(self, container_key, object_key, system_token=None):
        """
        delete an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.delete,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}",
        )
        return response

    def get_object_streaming(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store as a stream
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}",
            stream=True,
        )

        return response.iter_content(1024 * 1024)

    def get_object(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}",
        )
        return response.content

    def get_object_and_metadata(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store and also return header meta data

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}",
        )
        metadata = response.headers
        metadata["mime"] = metadata["Content-Type"]
        metadata["size"] = metadata["Content-Length"]
        return {"object": response.content, "metadata": metadata}

    def get_object_metadata(self, container_key, object_key, system_token=None):
        """
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return headers of the object
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}/meta",
        )
        result = response.json()
        result["Content-Type"] = result["mime"]  # backwards compatibility
        result["Content-Length"] = result["size"]  # backwards compatibility
        return result

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
        headers = {"content-type": "application/json"}
        object_data = {
            "host_url": self.host_url,
            "collection_key": self.collection,
            "container_key": source_container_key,
            "object_key": source_object_key,
        }
        response = self._execute_requests_method(
            requests.post,
            system_token,
            f"{self.base_url}/containers/{output_container_key}",
            response_code=201,
            headers=headers,
            json=object_data,
        )
        object_key = response.json()["object_key"]
        if isinstance(object_key, str):
            object_key = str(object_key)
        return object_key

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
        headers = {"content-type": "application/json"}
        object_data = {
            "host_url": self.host_url,
            "collection_key": self.collection,
            "container_key": source_container_key,
            "object_key": source_object_key,
        }
        self._execute_requests_method(
            requests.put,
            system_token,
            f"{self.base_url}/containers/{output_container_key}/{output_object_key}",
            headers=headers,
            json=object_data,
        )

    def update_object_and_key(self, container_key, object_data, system_token=None):
        """
        create an object and key in the data store

         :param container_key: key of the container in the data store
         :param object_data: data of the object
         :param system_token: oauth system token
         :raises InvalidStateException: if the response is in an invalid state
        """
        headers = {"content-type": "application/octet-stream"}
        response = self._execute_requests_method(
            requests.post,
            system_token,
            f"{self.base_url}/containers/{container_key}",
            response_code=201,
            headers=headers,
            data=object_data,
        )
        object_key = response.json()["object_key"]
        if isinstance(object_key, str):
            object_key = str(object_key)
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
        headers = {"content-type": "application/octet-stream"}
        return self._execute_requests_method(
            requests.put,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}",
            headers=headers,
            data=object_data,
        )

    def list_object_keys_for_container(self, container_key, system_token=None):
        """
        list all object keys for a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :return list of object keys found in the container
        :raises InvalidStateException: if the response is in an invalid state
        """
        headers = {"Accept": "application/json"}
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}",
            headers=headers,
        )
        return response.content

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
        translations = translations or {}
        headers = {"Accept": "application/zip"}
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}",
            headers=headers,
            stream=True,
            params=translations,
        )
        return response.iter_content(1024 * 1024)

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
        headers = {"Accept": "application/zip"}
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}",
            headers=headers,
            params=translations,
        )
        return response.content

    def create_container(self, container_key, system_token=None):
        """
        create a new container with specific key in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        return self._execute_requests_method(
            requests.put,
            system_token,
            f"{self.base_url}/containers/{container_key}",
        )

    def create_container_and_key(self, system_token=None):
        """
        create a new container in the data store and generate key

        :param system_token: oauth system token
        :return the key generated for the container
        :raises InvalidStateException: if the response is in an invalid state
        """

        response = self._execute_requests_method(
            requests.post,
            system_token,
            f"{self.base_url}/containers",
            response_code=201,
        )

        container_key = response.json()["container_key"]
        if isinstance(container_key, str):
            container_key = str(container_key)
        return container_key

    def delete_container(self, container_key, system_token=None):
        """
        delete a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        """
        return self._execute_requests_method(
            requests.delete,
            system_token,
            f"{self.base_url}/containers/{container_key}",
        )

    def get_object_from_archive(
        self,
        container_key,
        object_key,
        file_name,
        system_token=None
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
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}/{file_name}",
        )

        return response.content

    def get_object_from_archive_streaming(
        self,
        container_key,
        object_key,
        file_name,
        system_token=None
    ):
        """
        retrieve an object from an archive in the data storeas a stream
        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object as a stream
        :raises InvalidStateException: if the response is in an invalid state
        """
        response = self._execute_requests_method(
            requests.get,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}/{file_name}",
            stream=True,
        )

        return response.iter_content(1024 * 1024)

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
        response = self._execute_requests_method(
            requests.put,
            system_token,
            f"{self.base_url}/containers/{container_key}/{object_key}/{file_to_replace}",
            data=new_file_content,
            params={"new_file_name": new_file_name},
        )
        return response.json()


class InvalidStateException(Exception):
    def __init__(self, status_code, message="response has invalid state"):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return self.message + ", http status code: " + repr(self.status_code)
