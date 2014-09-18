# -*- coding: utf-8 -*-

import os
import unittest
from storageprovider.client import StorageProviderClient, InvalidStateException

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch  # pragma: no cover


test_container_key = 'test_container_key'
test_object_key = 'test_object_key'
test_base_url = 'http://localhost:6543/container'

here = os.path.dirname(__file__)


class StorageProviderTest(unittest.TestCase):
    def setUp(self):
        self.storageproviderclient = StorageProviderClient(test_base_url)

    def tearDown(self):
        pass

    @patch('storageprovider.client.requests')
    def test_delete_object(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_object(test_container_key, test_object_key)
        mock_requests.delete.assert_called_with(test_base_url + '/' + test_container_key + '/' + test_object_key)

    @patch('storageprovider.client.requests')
    def test_delete_object_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.delete.return_value.status_code = 400
        try:
            self.storageproviderclient.delete_object(test_container_key, test_object_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_get_object(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_object(test_container_key, test_object_key)
        mock_requests.get.assert_called_with(test_base_url + '/' + test_container_key + '/' + test_object_key)

    @patch('storageprovider.client.requests')
    def test_get_object_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.get.return_value.status_code = 400
        try:
            self.storageproviderclient.get_object(test_container_key, test_object_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_update_object(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.put.return_value.status_code = 200
        bin_file = None
        with open(kasteel, 'rb') as f:
            bin_file = f.read()
        self.storageproviderclient.update_object(test_container_key, test_object_key, bin_file)
        mock_requests.put.assert_called_with(test_base_url + '/' + test_container_key + '/' + test_object_key, bin_file,
                                             headers={'content-type': 'application/octet-stream'})

    @patch('storageprovider.client.requests')
    def test_update_object_KO(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        error_thrown = False
        error = None
        mock_requests.put.return_value.status_code = 400
        try:
            with open(kasteel, 'rb') as f:
                self.storageproviderclient.update_object(test_container_key, test_object_key, f.read())
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.list_object_keys_for_container(test_container_key)
        mock_requests.get.assert_called_with(test_base_url + '/' + test_container_key)

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.get.return_value.status_code = 400
        try:
            self.storageproviderclient.list_object_keys_for_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_create_container(self, mock_requests):
        mock_requests.put.return_value.status_code = 200
        self.storageproviderclient.create_container(test_container_key)
        mock_requests.put.assert_called_with(test_base_url + '/' + test_container_key)

    @patch('storageprovider.client.requests')
    def test_create_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.put.return_value.status_code = 400
        try:
            self.storageproviderclient.create_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': u'jk455'})
        res = self.storageproviderclient.create_container_and_key()
        mock_requests.post.assert_called_with(test_base_url)
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_no_unicode(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': 'jk455'})
        res = self.storageproviderclient.create_container_and_key()
        mock_requests.post.assert_called_with(test_base_url)
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.post.return_value.status_code = 400
        try:
            self.storageproviderclient.create_container_and_key()
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_delete_container(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_container(test_container_key)
        mock_requests.delete.assert_called_with(test_base_url + '/' + test_container_key)

    @patch('storageprovider.client.requests')
    def test_delete_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.delete.return_value.status_code = 400
        try:
            self.storageproviderclient.delete_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)
        self.assertEqual('response has invalid state, http status code: 400', str(error))


class PrefixedStorageProviderTest(unittest.TestCase):
    def setUp(self):
        self.container_prefix = "prefix"
        self.storageproviderclient = StorageProviderClient(test_base_url, self.container_prefix)

    def prefix(self, key):
        return ":".join([self.container_prefix, key])

    def tearDown(self):
        pass

    @patch('storageprovider.client.requests')
    def test_delete_object(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_object(test_container_key, test_object_key)
        mock_requests.delete.assert_called_with(test_base_url + '/' + self.prefix(test_container_key) + '/' + test_object_key)

    @patch('storageprovider.client.requests')
    def test_delete_object_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.delete.return_value.status_code = 400
        try:
            self.storageproviderclient.delete_object(test_container_key, test_object_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_get_object(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_object(test_container_key, test_object_key)
        mock_requests.get.assert_called_with(test_base_url + '/' + self.prefix(test_container_key) + '/' + test_object_key)

    @patch('storageprovider.client.requests')
    def test_get_object_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.get.return_value.status_code = 400
        try:
            self.storageproviderclient.get_object(test_container_key, test_object_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_update_object(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.put.return_value.status_code = 200
        bin_file = None
        with open(kasteel, 'rb') as f:
            bin_file = f.read()
        self.storageproviderclient.update_object(test_container_key, test_object_key, bin_file)
        mock_requests.put.assert_called_with(test_base_url + '/' + self.prefix(test_container_key) + '/' + test_object_key, bin_file,
                                             headers={'content-type': 'application/octet-stream'})

    @patch('storageprovider.client.requests')
    def test_update_object_KO(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        error_thrown = False
        error = None
        mock_requests.put.return_value.status_code = 400
        try:
            with open(kasteel, 'rb') as f:
                self.storageproviderclient.update_object(test_container_key, test_object_key, f.read())
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.list_object_keys_for_container(test_container_key)
        mock_requests.get.assert_called_with(test_base_url + '/' + self.prefix(test_container_key))

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.get.return_value.status_code = 400
        try:
            self.storageproviderclient.list_object_keys_for_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_create_container(self, mock_requests):
        mock_requests.put.return_value.status_code = 200
        self.storageproviderclient.create_container(test_container_key)
        mock_requests.put.assert_called_with(test_base_url + '/' + self.prefix(test_container_key))

    @patch('storageprovider.client.requests')
    def test_create_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.put.return_value.status_code = 400
        try:
            self.storageproviderclient.create_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': u'jk455'})
        res = self.storageproviderclient.create_container_and_key()
        mock_requests.post.assert_called_with(test_base_url)
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_no_unicode(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': 'jk455'})
        res = self.storageproviderclient.create_container_and_key()
        mock_requests.post.assert_called_with(test_base_url)
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.post.return_value.status_code = 400
        try:
            self.storageproviderclient.create_container_and_key()
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_delete_container(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_container(test_container_key)
        mock_requests.delete.assert_called_with(test_base_url + '/' + self.prefix(test_container_key))

    @patch('storageprovider.client.requests')
    def test_delete_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.delete.return_value.status_code = 400
        try:
            self.storageproviderclient.delete_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)
        self.assertEqual('response has invalid state, http status code: 400', str(error))