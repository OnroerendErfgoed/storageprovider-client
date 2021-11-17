import os
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from storageprovider.client import InvalidStateException
from storageprovider.client import StorageProviderClient

test_collection_key = 'test_collection'
test_container_key = 'test_container_key'
test_object_key = 'test_object_key'
test_base_url = 'http://localhost:6543'
test_check_url = test_base_url + '/collections/' + test_collection_key

here = os.path.dirname(__file__)


class AnyObject():
    def __init__(self):
        pass

    def __eq__(self, other):
        return other is not None


class StorageProviderTest(unittest.TestCase):
    def setUp(self):
        self.storageproviderclient = StorageProviderClient(test_base_url, test_collection_key)

    def tearDown(self):
        pass

    @patch('storageprovider.client.requests')
    def test_delete_object(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_object(test_container_key, test_object_key)
        mock_requests.delete.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key, headers={})

    @patch('storageprovider.client.requests')
    def test_delete_object_system_token(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_object(test_container_key, test_object_key, "x123-test")
        mock_requests.delete.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key,
            headers={"OpenAmSSOID": "x123-test"})

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
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key, headers={})

    @patch('storageprovider.client.requests')
    def test_get_object_system_token(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_object(test_container_key, test_object_key, "x123-test")
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key,
            headers={"OpenAmSSOID": "x123-test"})

    @patch('storageprovider.client.requests')
    def test_get_object_custom_system_token(self, mock_requests):
        storageproviderclient = StorageProviderClient(test_base_url, test_collection_key,
                                                      system_token_header="system_token")
        mock_requests.get.return_value.status_code = 200
        storageproviderclient.get_object(test_container_key, test_object_key, "x123-test")
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key,
            headers={"system_token": "x123-test"})

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
    def test_get_object_and_metadata(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        object_dict = self.storageproviderclient.get_object_and_metadata(test_container_key, test_object_key)
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key, headers={})
        self.assertIn('object', object_dict)
        self.assertIn('metadata', object_dict)

    @patch('storageprovider.client.requests')
    def test_get_object_metadata(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        mock_requests.get.return_value.json.return_value = {
            "time_last_modification": "2016-03-30T09:51:45",
            "size": 22878,
            "mime": "application/pdf",
        }
        result = self.storageproviderclient.get_object_metadata(
            test_container_key, test_object_key
        )
        mock_requests.get.assert_called_with(
            f"{test_check_url}/containers/{test_container_key}/{test_object_key}/meta",
            headers={}
        )
        self.assertEqual(
            {
                'time_last_modification': '2016-03-30T09:51:45',
                'size': 22878,
                'mime': 'application/pdf',
                'Content-Type': 'application/pdf',
                'Content-Length': 22878
            },
            result
        )

    @patch('storageprovider.client.requests')
    def test_get_object_metadata_system_token(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        mock_requests.get.return_value.json.return_value = {
            "time_last_modification": "2016-03-30T09:51:45",
            "size": 22878,
            "mime": "application/pdf",
        }
        result = self.storageproviderclient.get_object_metadata(
            test_container_key, test_object_key, system_token="x123-test"
        )
        mock_requests.get.assert_called_with(
            f"{test_check_url}/containers/{test_container_key}/{test_object_key}/meta",
            headers={'OpenAmSSOID': 'x123-test'}
        )
        self.assertEqual(
            {
                'time_last_modification': '2016-03-30T09:51:45',
                'size': 22878,
                'mime': 'application/pdf',
                'Content-Type': 'application/pdf',
                'Content-Length': 22878
            },
            result
        )

    @patch('storageprovider.client.requests')
    def test_get_object_metadata_KO(self, mock_requests):
        mock_requests.head.return_value.status_code = 400
        self.assertRaises(InvalidStateException, self.storageproviderclient.get_object_metadata, test_container_key,
                          test_object_key)

    @patch('storageprovider.client.requests')
    def test_update_object(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.put.return_value.status_code = 200
        bin_file = None
        with open(kasteel, 'rb') as f:
            bin_file = f.read()
        self.storageproviderclient.update_object(test_container_key, test_object_key, bin_file)
        mock_requests.put.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key, data=AnyObject(),
            headers={'content-type': 'application/octet-stream'})

    @patch('storageprovider.client.requests')
    def test_update_object_system_token(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.put.return_value.status_code = 200
        with open(kasteel, 'rb') as f:
            self.storageproviderclient.update_object(test_container_key, test_object_key, f, "x123-test")
        mock_requests.put.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key, data=AnyObject(),
            headers={"content-type": "application/octet-stream", "OpenAmSSOID": "x123-test"})

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
    def test_create_object_and_key_system_token(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'object_key': u'jk455'})
        with open(kasteel, 'rb') as f:
            res = self.storageproviderclient.update_object_and_key(test_container_key, f, "x123-test")
        mock_requests.post.assert_called_with(
            test_check_url + '/containers/' + test_container_key, data=AnyObject(),
            headers={"content-type": "application/octet-stream", "OpenAmSSOID": "x123-test"})
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_object_and_key_KO(self, mock_requests):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        mock_requests.post.return_value.status_code = 400
        mock_requests.post.return_value.json = Mock(return_value={'object_key': u'jk455'})
        with open(kasteel, 'rb') as f:
            self.assertRaises(InvalidStateException, self.storageproviderclient.update_object_and_key,
                              test_container_key, f, "x123-test")

    @patch('storageprovider.client.requests')
    def test_copy_object_create_key_system_token(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'object_key': u'jk455'})
        res = self.storageproviderclient.copy_object_and_create_key(
            "source_container_key", "source_object_key", test_container_key, "x123-test")
        mock_requests.post.assert_called_with(
            test_check_url + '/containers/' + test_container_key,
            json={
                'host_url': test_base_url,
                'collection_key': test_collection_key,
                'container_key': 'source_container_key',
                'object_key': 'source_object_key'
            },
            headers={"content-type": "application/json", "OpenAmSSOID": "x123-test"})
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_copy_object_create_key_KO(self, mock_requests):
        mock_requests.post.return_value.status_code = 400
        mock_requests.post.return_value.json = Mock(return_value={'object_key': u'jk455'})
        self.assertRaises(InvalidStateException, self.storageproviderclient.copy_object_and_create_key,
                          "source_container_key", "source_object_key", test_container_key, "x123-test")

    @patch('storageprovider.client.requests')
    def test_copy_object_system_token(self, mock_requests):
        mock_requests.put.return_value.status_code = 200
        self.storageproviderclient.copy_object("source_container_key", "source_object_key",
                                               test_container_key, test_object_key, "x123-test")
        mock_requests.put.assert_called_with(
            test_check_url + '/containers/' + test_container_key + '/' + test_object_key,
            json={
                'host_url': test_base_url,
                'collection_key': test_collection_key,
                'container_key': 'source_container_key',
                'object_key': 'source_object_key'
            },
            headers={"content-type": "application/json", "OpenAmSSOID": "x123-test"})

    @patch('storageprovider.client.requests')
    def test_copy_object_KO(self, mock_requests):
        mock_requests.put.return_value.status_code = 400
        self.assertRaises(InvalidStateException, self.storageproviderclient.copy_object,
                          "source_container_key", "source_object_key",
                          test_container_key, test_object_key, "x123-test")

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.list_object_keys_for_container(test_container_key)
        mock_requests.get.assert_called_with(test_check_url + '/containers/'
                                             + test_container_key, headers={'Accept': 'application/json'})

    @patch('storageprovider.client.requests')
    def test_list_object_keys_for_container_system_token(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.list_object_keys_for_container(test_container_key, "x123-test")
        mock_requests.get.assert_called_with(test_check_url + '/containers/' + test_container_key,
                                             headers={'Accept': 'application/json',"OpenAmSSOID": "x123-test"})

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
    def test_get_container_data(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_container_data(test_container_key)
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key,
            headers={'Accept': 'application/zip'},
            params={}
        )

    @patch('storageprovider.client.requests')
    def test_get_container_data_translations(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_container_data(
            test_container_key, translations={'001': 'filename.pdf'}
        )
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key,
            headers={'Accept': 'application/zip'},
            params={'001': 'filename.pdf'}
        )

    @patch('storageprovider.client.requests')
    def test_get_container_system_token(self, mock_requests):
        mock_requests.get.return_value.status_code = 200
        self.storageproviderclient.get_container_data(test_container_key,
                                                      "x123-test")
        mock_requests.get.assert_called_with(
            test_check_url + '/containers/' + test_container_key,
            headers={'Accept': 'application/zip', "OpenAmSSOID": "x123-test"},
            params={}
        )

    @patch('storageprovider.client.requests')
    def test_get_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.get.return_value.status_code = 400
        try:
            self.storageproviderclient.get_container_data(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)

    @patch('storageprovider.client.requests')
    def test_create_container(self, mock_requests):
        mock_requests.put.return_value.status_code = 200
        self.storageproviderclient.create_container(test_container_key)
        mock_requests.put.assert_called_with(test_check_url + '/containers/' + test_container_key, headers={})

    @patch('storageprovider.client.requests')
    def test_create_container_system_token(self, mock_requests):
        mock_requests.put.return_value.status_code = 200
        self.storageproviderclient.create_container(test_container_key, "x123-test")
        mock_requests.put.assert_called_with(test_check_url + '/containers/' + test_container_key,
                                             headers={"OpenAmSSOID": "x123-test"})

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
        mock_requests.post.assert_called_with(test_check_url + '/containers', headers={})
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_system_token(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': u'jk455'})
        res = self.storageproviderclient.create_container_and_key("x123-test")
        mock_requests.post.assert_called_with(test_check_url + '/containers', headers={"OpenAmSSOID": "x123-test"})
        self.assertEqual('jk455', res)

    @patch('storageprovider.client.requests')
    def test_create_container_and_key_no_unicode(self, mock_requests):
        mock_requests.post.return_value.status_code = 201
        mock_requests.post.return_value.json = Mock(return_value={'container_key': 'jk455'})
        res = self.storageproviderclient.create_container_and_key()
        mock_requests.post.assert_called_with(test_check_url + '/containers', headers={})
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
        mock_requests.delete.assert_called_with(test_check_url + '/containers/' + test_container_key, headers={})

    @patch('storageprovider.client.requests')
    def test_delete_container_system_token(self, mock_requests):
        mock_requests.delete.return_value.status_code = 200
        self.storageproviderclient.delete_container(test_container_key, "x123-test")
        mock_requests.delete.assert_called_with(test_check_url + '/containers/' + test_container_key,
                                                headers={"OpenAmSSOID": "x123-test"})

    @patch('storageprovider.client.requests')
    def test_delete_container_KO(self, mock_requests):
        error_thrown = False
        error = None
        mock_requests.delete.return_value.status_code = 400
        mock_requests.delete.return_value.text = 'test error'
        try:
            self.storageproviderclient.delete_container(test_container_key)
        except InvalidStateException as ise:
            error_thrown = True
            error = ise
        self.assertTrue(error_thrown)
        self.assertEqual(400, error.status_code)
        self.assertEqual('test error, http status code: 400', str(error))

    def test_read_in_chunks(self):
        kasteel = os.path.join(here, '..', 'fixtures/kasteel.jpg')
        with open(kasteel, 'rb') as f:
            gen = self.storageproviderclient._read_in_chunks(f)
            res = gen.__next__() if hasattr(gen, '__next__') else gen.next()
            self.assertIsNotNone(res)
            self.assertEqual(1024, len(res))
            for n in gen:
                self.assertIsNotNone(n)
