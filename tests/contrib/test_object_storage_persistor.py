"""
    test ibm object storage persistor
"""
import pytest
from unittest import mock
from ibmaemagic.contrib.ibm_object_storage_persistor import IBMObjectStoragePersistor


@pytest.fixture(scope='function')
def mock_class(request):
    # mock the persistor with fixed connection to ibm object storage
    config = '{}'
    client = IBMObjectStoragePersistor(config)
    return client

@pytest.fixture(scope='function')
def mock_file(request):
    # mock the feature meta data class
    return 'foo.py'


@pytest.fixture(scope='function')
def mock_config(request):
    # mock the config object
    config = {
                "api_key": "foobar_ibm_api_key_id",
                "endpoint": "foobar_endpoint",
                "ibm_auth_endpoint": "foobar_ibm_auth_endpoint",
                "bucket": "foobar_bucket"
             }
            
    return config



class TestIBMObjectStoragePersistor():
    def test_init_parse_config(self, mock_config):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mock.patch.object(mocked_boto,'client', return_value=None)  
            # act
            obj = IBMObjectStoragePersistor(config)
           
            # assert
            assert obj.IBM_API_KEY_ID == 'foobar_ibm_api_key_id'
            assert obj.ENDPOINT == 'https://foobar_endpoint'
            assert obj.IBM_AUTH_ENDPOINT == 'https://foobar_ibm_auth_endpoint'
            assert obj.BUCKET == 'foobar_bucket'

    def test_init_call_boto_client_once(self, mock_config):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            # act
            IBMObjectStoragePersistor(config) 
            # assert
            mocked_boto.client.assert_called_once()

    def test_init_call_boto_client_success(self, mock_config):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mocked_boto.client.return_value='client'   
            # act
            mock_persistor = IBMObjectStoragePersistor(config)
            # assert
            assert mock_persistor.client == 'client'

    def test_init_call_boto_client_exception(self, mock_config, capsys):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mocked_boto.client.side_effect = Exception('Test Exception')
            # act
            IBMObjectStoragePersistor(config)
            captured = capsys.readouterr()
            # assert
            assert captured.out[:5] == '> ibm'

    def test_write_method_call_put_object_once(self, mock_config, mock_file):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock.patch.object(mocked_boto.client,'put_object',return_value=None)
            # act
            mock_persistor.write(mock_file, None) 
            # assert
            mock_persistor.client.put_object.assert_called_once()
    
    def test_write_method_call_put_object_exception(self, mock_config, mock_file, capsys):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3'):
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock_persistor.client.put_object.side_effect = Exception('Test Exception')
    
            # act/assert
            with pytest.raises(Exception): 
                mock_persistor.client.put_object()
            mock_persistor.write(mock_file, None) # supposed to catch exception
            captured = capsys.readouterr()
            assert captured.out[:5] == '> ibm'

    def test_read_method_call_get_object_once(self, mock_config, mock_file):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock.patch.object(mocked_boto.client,'get_object',return_value=None)
            # act
            mock_persistor.read(mock_file) 
            # assert   
            mock_persistor.client.get_object.assert_called_once()

    def test_read_method_call_get_object_return(self, mock_config, mock_file):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3'):
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock_body = mock.Mock()
            mock_body.read.return_value = b'test body!'
            mock_persistor.client.get_object.return_value = {'Body':mock_body}
            # act
            body = mock_persistor.read(mock_file)
            # assert
            assert body == b'test body!'
    
    def test_read_method_call_get_object_exception(self, mock_config, mock_file, capsys):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3'):
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock_persistor.client.get_object.side_effect = Exception('Test Exception')
            # act/assert
            with pytest.raises(Exception): 
                mock_persistor.client.get_object()
            body = mock_persistor.read(mock_file) # supposed to catch exception
            captured = capsys.readouterr()
            assert captured.out[:5] == '> ibm'
            assert body == None
    
    def test_delete_method_call_delete_object_once(self, mock_config, mock_file):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock.patch.object(mocked_boto.client,'delete_object',return_value=None)
            # act
            mock_persistor.delete(mock_file)    
            # assert
            mock_persistor.client.delete_object.assert_called_once()

    def test_delete_method_call_delete_object_exception(self, mock_config, mock_file, capsys):
        with mock.patch('ibmaemagic.contrib.ibm_object_storage_persistor.ibm_boto3'):
            # arrange
            config = mock_config
            mock_persistor = IBMObjectStoragePersistor(config)
            mock_persistor.client.delete_object.side_effect = Exception('Test Exception')
            # act/assert
            with pytest.raises(Exception): 
                mock_persistor.client.delete_object()
            mock_persistor.delete(mock_file) # supposed to catch exception
            captured = capsys.readouterr()
            assert captured.out[:5] == '> ibm'

    

