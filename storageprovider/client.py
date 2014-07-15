# -*- coding: utf-8 -*-

import requests
from six import text_type


class StorageProviderClient(object):

    def __init__(self, base_url):
        self.base_url = base_url

    def delete_object(self, container_key, object_key):
        '''
        delete an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.delete(self.base_url + '/' + container_key + '/' + object_key)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)

    def get_object(self, container_key, object_key):
        '''
        retrieve an object from the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :return content of the object
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.get(self.base_url + '/' + container_key + '/' + object_key)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)
        return res.content

    def update_object(self, container_key, object_key, object_data):
        '''
        update (or create) an object in the data store

        :param container_key: key of the container in the data store
        :param object_key: specific object key for the object in the container
        :param object_data: data of the object
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.put(self.base_url + '/' + container_key + '/' + object_key, object_data)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)

    def list_object_keys_for_container(self, container_key):
        '''
        list all object keys for a container in the data store

        :param container_key: key of the container in the data store
        :return list of object keys found in the container
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.get(self.base_url + '/' + container_key)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)
        return res.content

    def create_container(self, container_key):
        '''
        create a new container with specific key in the data store

        :param container_key: key of the container in the data store
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.put(self.base_url + '/' + container_key)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)

    def create_container_and_key(self):
        '''
        create a new container in the data store and generate key

        :return the key generated for the container
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.post(self.base_url)
        if res.status_code != 201:
            raise InvalidStateException(res.status_code)
        container_key = res.json()['container_key']
        if isinstance(container_key, text_type):
            container_key = str(container_key)
        return container_key

    def delete_container(self, container_key):
        '''
        delete a container in the data store

        :param container_key: key of the container in the data store
        :raises InvalidStateException: if the response is in an invalid state
        '''
        res = requests.delete(self.base_url + '/' + container_key)
        if res.status_code != 200:
            raise InvalidStateException(res.status_code)


class InvalidStateException(Exception):
    def __init__(self, status_code, message='response has invalid state'):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return self.message + ', http status code: ' + repr(self.status_code)