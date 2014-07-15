# -*- coding: utf-8 -*-

import requests
from six import text_type


class StorageProviderClient(object):

    def __init__(self, base_url):
        self.base_url = base_url

    def delete_object(self, container_key, object_key):
        '''delete an object from the data store'''
        res = requests.delete(self.base_url + '/' + container_key + '/' + object_key)
        assert 200 == res.status_code

    def get_object(self, container_key, object_key):
        '''retrieve an object from the data store'''
        res = requests.get(self.base_url + '/' + container_key + '/' + object_key)
        assert 200 == res.status_code
        return res.content

    def update_object(self, container_key, object_key, object_data):
        '''update an object in the data store'''
        res = requests.put(self.base_url + '/' + container_key + '/' + object_key, object_data)
        assert 200 == res.status_code

    def list_object_keys_for_container(self, container_key):
        '''list all object keys for a container in the data store'''
        res = requests.get(self.base_url + '/' + container_key)
        assert 200 == res.status_code
        return res.content

    def create_container(self, container_key):
        '''create a new container in the data store'''
        res = requests.put(self.base_url + '/' + container_key)
        assert 200 == res.status_code

    def create_container_and_key(self):
        '''create a new container in the data store'''
        res = requests.post(self.base_url)
        assert 201 == res.status_code
        container_key = res.json()['container_key']
        if isinstance(container_key, text_type):
            container_key = str(container_key)
        return container_key

    def delete_container(self, container_key):
        '''delete a container in the data store'''
        res = requests.delete(self.base_url + '/' + container_key)
        assert 200 == res.status_code