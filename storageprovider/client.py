# -*- coding: utf-8 -*-

import requests
from six import text_type


class StorageProviderClient(object):

    def __init__(self, base_url, system_token_header='OpenAmSSOID'):
        self.base_url = base_url
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
        res = requests.delete(self.base_url + '/' + container_key + '/' + object_key, headers=headers)
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
        res = requests.get(self.base_url + '/' + container_key + '/' + object_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)
        return res.content

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
        res = requests.put(self.base_url + '/' + container_key + '/' + object_key,
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
        res = requests.get(self.base_url + '/' + container_key, headers=headers)
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
        res = requests.put(self.base_url + '/' + container_key, headers=headers)
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
        res = requests.post(self.base_url, headers=headers)
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
        res = requests.delete(self.base_url + '/' + container_key, headers=headers)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code, res.text)


class InvalidStateException(Exception):
    def __init__(self, status_code, message='response has invalid state'):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return self.message + ', http status code: ' + repr(self.status_code)