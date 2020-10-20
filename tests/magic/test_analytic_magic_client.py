import pytest
import os
import json
from unittest import mock
from ibmaemagic.magic.analytic_magic_client import AnalyticMagicClient


@pytest.fixture(scope='function')
def mock_client(monkeypatch):
    monkeypatch.setenv('USER_ACCESS_TOKEN', 'ibm_fake_token')
    AnalyticMagicClient.init('https://foobar.com')
    yield AnalyticMagicClient
    AnalyticMagicClient.reset()


class TestAnalyticMagicClient():


    def test_init_with_valid_param(self, monkeypatch):
        # arrange
        host = 'https://www.foo.com'
        param_list = [{'uid':'foo', 'pwd':'bar', 'env':None},
                      {'uid':None, 'pwd':None, 'env':'env_auth_token'},
                      {'uid':'foo', 'pwd':'bar', 'env':'env_auth_token'}]
        token_list = ['ibm_auth_token','env_auth_token', 'ibm_auth_token']
        for i,param in enumerate(param_list):
            uid, pwd, env = param['uid'], param['pwd'], param['env']
            if env: # set enviroment variable
                monkeypatch.setenv('USER_ACCESS_TOKEN', env)
            with mock.patch('http.client.HTTPSConnection') as mock_conn: # mock http client
                ibm_auth_response = json.dumps({'accessToken':'ibm_auth_token'})
                mock_conn().getresponse().read().decode= mock.MagicMock(return_value=ibm_auth_response)
                # act
                AnalyticMagicClient.init(host, uid=uid, pwd=pwd, verbose=False)
                # assert
                assert AnalyticMagicClient.token == token_list[i]
                assert AnalyticMagicClient.host == host
            if 'USER_ACCESS_TOKEN' in os.environ:
                 monkeypatch.delenv('USER_ACCESS_TOKEN')
            AnalyticMagicClient.reset()

    def test_init_with_invalid_param(self, monkeypatch):
        # arange
        param_list = [{'host':'https://www.foobar.com', 'env':None},
                      {'host': None, 'env': 'fake_auth_token'},
                      {'host': None, 'env': None}]
        # act & assert
        for param in param_list:
            host, env = param['host'], param['env']
            if env: # set enviroment variable
                monkeypatch.setenv('USER_ACCESS_TOKEN', env)
            with pytest.raises(ValueError):
                AnalyticMagicClient.init(host)
            if 'USER_ACCESS_TOKEN' in os.environ:
                monkeypatch.delenv('USER_ACCESS_TOKEN')
            AnalyticMagicClient.reset()

    def test_connection_ready(self, mock_client):
        # arange
        mock_client
        # act
        mock_client.token = 'some token'
        # assert
        mock_client.connection_ready == True

    def test_connection_not_ready(self, mock_client):
        # arange
        mock_client
        # act
        mock_client.token = None
        # assert
        mock_client.connection_ready == False





