"""
"
" test watson service transformer class
"
"""
import os
import pytest
import json
from unittest import mock
from ibmaemagic.sdk.analytic_engine_client import AnalyticEngineClient



@pytest.fixture(scope='function')
def mock_client(request):
    # define the mock of analytic engine client
    host = 'https://www.foo.com'
    token = 'fake_auth_token'
    client = AnalyticEngineClient(host, token=token)
    return client



class TestAnalyticEngineClient():

    def test_init_with_valid_param(self):
        # arrange
        host = 'https://www.foo.com'
        param_list = [{'token':'fake_auth_token', 'uid':None, 'pwd':None, 'env':None},
                      {'token':None, 'uid':'foo', 'pwd':'bar', 'env':None},
                      {'token':None, 'uid':None, 'pwd':None, 'env':'env_fake_token'}]
        token_list = ['fake_auth_token', 'ibm_auth_token','env_fake_token']
        for i,param in enumerate(param_list):
            token, uid, pwd, env = param['token'], param['uid'], param['pwd'], param['env']

            if env: # set enviroment variable
                os.environ['USER_ACCESS_TOKEN'] = env

            with mock.patch('http.client.HTTPSConnection') as mock_conn: # mock http client
                ibm_auth_response = json.dumps({'accessToken':'ibm_auth_token'})
                mock_conn().getresponse().read().decode= mock.MagicMock(return_value=ibm_auth_response)
                # act
                client = AnalyticEngineClient(host, token=token, uid=uid, pwd=pwd, verbose=False)
                # assert
                assert client.token == token_list[i]
                assert client.host == host

            if 'USER_ACCESS_TOKEN' in os.environ:
                del os.environ['USER_ACCESS_TOKEN'] 

    def test_init_with_invalid_param(self):
        # arange
        param_list = [{'host':'https://www.foobar.com', 'token':None},
                      {'host': None, 'token': 'fake_auth_token'},
                      {'host': None, 'token': None}]

        # act & assert
        for param in param_list:
            host, token = param['host'], param['token']
            with pytest.raises(Exception):
                AnalyticEngineClient(host, token=token)

    def test_get_all_instances(self, mock_client):
        # arrange
        json_message = json.dumps({"instances":["foo1","foo2","foo3"]})
        mock_client.__GET__ = mock.MagicMock(return_value=json_message)
        # act
        result = mock_client.get_all_instances()
        # assert
        assert isinstance(result, dict)
        assert isinstance(result["instances"], list)
        assert len(result["instances"]) == 3
        assert result["instances"][0] == 'foo1'

    def test_get_all_instances_with_error(self, mock_client):
        # arrange
        mock_client.__GET__ = mock.MagicMock(side_effect=Exception('> Get request error.'))
        # act & assert
        with pytest.raises(Exception):
            mock_client.get_all_instances()
        mock_client.__GET__.assert_called_once()


    