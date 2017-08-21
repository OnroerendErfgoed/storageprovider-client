# -*- coding: utf-8 -*-

import requests
from six import text_type


class StorageProviderClient(object):

    def __init__(self, base_url, collection, system_token_header='OpenAmSSOID'):
        self.host_url = base_url
        self.base_url = base_url + '/collections/' + collection
        self.collection = collection
        self.system_token_header = system_token_header

    @staticmethod
    def _read_in_chunks(file_object, chunk_size=1024):
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def delete_object(self, container_key, object_key, system_token=None):
        '''
        delete an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.delete(self.base_url + '/containers/' + container_key + '/' + object_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)

    def get_object(self, container_key, object_key, system_token=None):
        '''
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.get(self.base_url + '/containers/' + container_key + '/' + object_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)
        return res.content

    def get_object_metadata(self, container_key, object_key, system_token=None):
        '''
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param system_token: oauth system token
        :return headers of the object
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.head(self.base_url + '/containers/' + container_key + '/' + object_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)
        return res.headers

    def copy_object_and_create_key(self, source_container_key, source_object_key, output_container_key,
                                   system_token=None):
        '''
        Copy an object and create key in the data store

        :param source_container_key: key of the source container in the data store
        :param source_object_key: key of the source object in the container
        :param output_container_key: key of output container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {'content-type': 'application/json'}
        if system_token:
            headers[self.system_token_header] = system_token
        object_data = {
            'host_url': self.host_url,
            'collection_key': self.collection,
            'container_key': source_container_key,
            'object_key': source_object_key
        }
        res = requests.post(self.base_url + '/containers/' + output_container_key, json=object_data, headers=headers)
        if res.status_code != 201:
            raise InvalidStateException(res.status_code, res.text)
        object_key = res.json()['object_key']
        if isinstance(object_key, text_type):
            object_key = str(object_key)
        return object_key

    def copy_object(self, source_container_key, source_object_key, output_container_key, output_object_key,
                    system_token=None):
        '''
        Copy an object in the data store to specific key

        :param source_container_key: key of the source container in the data store
        :param source_object_key: key of the source object in the container
        :param output_container_key: key of output container in the data store
        :param output_object_key: specific object key for the output object in the container
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {'content-type': 'application/json'}
        if system_token:
            headers[self.system_token_header] = system_token
        object_data = {
            'host_url': self.host_url,
            'collection_key': self.collection,
            'container_key': source_container_key,
            'object_key': source_object_key
        }
        res = requests.put(self.base_url + '/containers/' + output_container_key + '/' + output_object_key,
                           json=object_data, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)

    def update_object_and_key(self, container_key, object_data, system_token=None):
        '''
       create an object and key in the data store

        :param container_key: key of the container in the data store
        :param object_data: data of the object
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {'content-type': 'application/octet-stream'}
        if system_token:
            headers[self.system_token_header] = system_token
        res = requests.post(self.base_url + '/containers/' + container_key, data=object_data, headers=headers)
        if res.status_code != 201:
            raise InvalidStateException(res.status_code, res.text)
        object_key = res.json()['object_key']
        if isinstance(object_key, text_type):
            object_key = str(object_key)
        return object_key

    def update_object(self, container_key, object_key, object_data, system_token=None):
        '''
        update (or create) an object in the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param object_data: data of the object
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {'content-type': 'application/octet-stream'}
        if system_token:
            headers[self.system_token_header] = system_token
        res = requests.put(self.base_url + '/containers/' + container_key + '/' + object_key,
                           data=object_data, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)

    def list_object_keys_for_container(self, container_key, system_token=None):
        '''
        list all object keys for a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :return list of object keys found in the container
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.get(self.base_url + '/containers/' + container_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)
        return res.content

    def create_container(self, container_key, system_token=None):
        '''
        create a new container with specific key in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.put(self.base_url + '/containers/' + container_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)

    def create_container_and_key(self, system_token=None):
        '''
        create a new container in the data store and generate key

        :param system_token: oauth system token
        :return the key generated for the container
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.post(self.base_url + '/containers', headers=headers)
        if res.status_code != 201:
            raise InvalidStateException(res.status_code, res.text)
        container_key = res.json()['container_key']
        if isinstance(container_key, text_type):
            container_key = str(container_key)
        return container_key

    def delete_container(self, container_key, system_token=None):
        '''
        delete a container in the data store

        :param container_key: key of the container in the data store
        :param system_token: oauth system token
        :raises InvalidStateException: if the response is in an invalid state
        '''
        headers = {}
        if system_token:
            headers = {self.system_token_header: system_token}
        res = requests.delete(self.base_url + '/containers/' + container_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)


class InvalidStateException(Exception):
    def __init__(self, status_code, message='response has invalid state'):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return self.message + ', http status code: ' + repr(self.status_code)