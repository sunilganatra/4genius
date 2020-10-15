#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import sys
sys.path.insert(1, '../src/genius4/sdk/')
from ae_client import  AnalyticsEngineClient
from uuid import uuid4
import json
import time
import warnings
import traceback
from unittest.mock import patch
import fake_json as fj

# warnings.simplefilter("ignore", ResourceWarning)
# unittest.main(warnings='ignore')

HOST = ""
UID = ""
PWD = ""
unique_id = str(uuid4()).split("-")[0]
INSTANCE_NAME = "unit-test-instance-"+unique_id 
VOLUME_NAME = "unit-test-volume-"+unique_id
VOLUME_ID = None #this will be filled while creating volume instance
SLEEP_DURATION = 1
DATA_FOLDER_PATH ="../data/" 
PYTHON_FILE = "sparkify.py"
SAMPLE_FILE = "sample_data.csv"

print("VOLUME NAME = {}".format(VOLUME_NAME))

analytics_engine = AnalyticsEngineClient( host=HOST, uid=UID, pwd=PWD)


class TestClient(unittest.TestCase):
    VOLUME_ID = None #this will be filled while creating volume instance
    # def __init__(self, temp):
    #     HOST = "cp4d-cpd-cp4d.pathfinder-royalbankofc-73aebe06726e634c608c4167edcc2aeb-0000.tor01.containers.appdomain.cloud"
    #     UID = "admin"
    #     PWD = "rbccp4d-rbccp4d"
    #     self.analytics_engine = AnalyticsEngineClient( host=HOST, uid=UID, pwd=PWD)

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        warnings.simplefilter("default", ResourceWarning)

    def test_get_all_instances(self):

        fake_json = fj.message_code_success
        with patch('ae_client.AnalyticsEngineClient.get_all_instances') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.get_all_instances()
            self.assertIsInstance(result, dict)
            self.assertEqual(result["_messageCode_"], 'success')
    
    def test_get_all_volumes(self):
        fake_json = fj.message_code_success
        with patch('ae_client.AnalyticsEngineClient.get_all_volumes') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.get_all_volumes()
            self.assertIsInstance(result, dict)
            self.assertEqual(result["_messageCode_"], 'success')

    def test_get_all_storage_class(self):
        fake_json = fj.get_all_storage_class_json
        with patch('ae_client.AnalyticsEngineClient.get_all_storage_class') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.get_all_storage_class()
            self.assertIsInstance(result, list)

    def test_create_volume(self):
        with self.assertRaises(TypeError):  
            analytics_engine.create_volume()

        #creating volume - without passing create_arguments
        with self.assertRaises(Exception):  
            result = analytics_engine.create_volume(VOLUME_NAME)

        #creating volume - with empty create_arguments
        with self.assertRaises(Exception):  
            result = analytics_engine.create_volume(VOLUME_NAME, create_arguments={})

        #creating volume - with create_arguments
        create_arguments = {
            "metadata": {
                "storageClass": "default",
                "storageSize": "1Gi"
            },
            "resources": {},
            "serviceInstanceDescription": "Volume created from unit test cases"
        }
        fake_json = fj.create_volume_json
        with patch('ae_client.AnalyticsEngineClient.create_volume') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.create_volume(VOLUME_NAME, create_arguments=create_arguments)
            self.assertIsInstance(result, dict)
            self.assertEqual(result["_messageCode_"], "200")
            self.assertEqual(result["id"], "1602689675351")
            self.assertEqual(result["message"], "Started provisioning the instance")

    def test_start_volume(self):
        time.sleep(SLEEP_DURATION)
        #start volume
        fake_json = fj.message_code_200
        with patch('ae_client.AnalyticsEngineClient.start_volume') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.start_volume(VOLUME_NAME)
            self.assertIsInstance(result, dict)
            self.assertEqual(result["_messageCode_"], "200")

    
    def test_volume_status(self):
        fake_json = fj.get_volume_status_json
        with patch('ae_client.AnalyticsEngineClient.get_volume_status') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.get_volume_status(volume_name=VOLUME_NAME)
            self.assertEqual(result["status"], "PROVISIONED")

    def test_add_file_to_volume(self):
        with self.assertRaises(TypeError):  
            result = analytics_engine.add_file_to_volume()
        
        time.sleep(SLEEP_DURATION)
        with self.assertRaises(TypeError):  
            result = analytics_engine.add_file_to_volume(volume_name="test")

        time.sleep(SLEEP_DURATION)
        with self.assertRaises(TypeError):  
            result = analytics_engine.add_file_to_volume(volume_name="test", source_file = "test")

        time.sleep(SLEEP_DURATION)
        with self.assertRaises(FileNotFoundError):  
            result = analytics_engine.add_file_to_volume(volume_name="test", source_file = str(uuid4()), target_file_name= "test1")
        
        fake_json = fj.message_code_success
        with patch('ae_client.AnalyticsEngineClient.add_file_to_volume') as mock_get:
            mock_get.return_value= fake_json
         
            #when volume is ready- start moving files to volume
            source_file = DATA_FOLDER_PATH+PYTHON_FILE
            result = analytics_engine.add_file_to_volume(volume_name=VOLUME_NAME, source_file = source_file, target_file_name= PYTHON_FILE)
            self.assertEqual(result["_messageCode_"], "success")

            source_file = DATA_FOLDER_PATH+SAMPLE_FILE
            result = analytics_engine.add_file_to_volume(volume_name=VOLUME_NAME, source_file = source_file, target_file_name= SAMPLE_FILE)
            self.assertEqual(result["_messageCode_"], "success")

    def test_create_ae_instance(self):

        with self.assertRaises(TypeError):  
            result = analytics_engine.create_instance()

        with self.assertRaises(Exception):  
            result = analytics_engine.create_instance("some-random-name")

        with self.assertRaises(Exception):  
            result = analytics_engine.create_instance(INSTANCE_NAME, create_arguments={})

        create_arguments_instance = {
            "metadata":{
            "volumeName": "unit-test-instance-volume-"+unique_id,
            "storageClass": "",
            "storageSize": ""
            },
            "serviceInstanceDescription": "Instance volume created for unit test"
        }    

        fake_json = fj.message_code_200
        with patch('ae_client.AnalyticsEngineClient.create_instance') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.create_instance(INSTANCE_NAME, create_arguments=create_arguments_instance)
            self.assertIsInstance(result, dict)
            self.assertEqual(result["_messageCode_"], "200")

    def test_submit_word_count(self):
        fake_json = fj.submit_job_json
        with patch('ae_client.AnalyticsEngineClient.submit_word_count_job') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.submit_word_count_job(INSTANCE_NAME)
            self.assertIsInstance(result, dict)
            self.assertEqual(result["id"], fake_json["id"])
            self.assertEqual(result["job_state"], fake_json["job_state"])

    
    def test_start_history_server(self):
        fake_json = fj.start_history_server_json
        with patch('ae_client.AnalyticsEngineClient.start_history_server') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.start_history_server(INSTANCE_NAME)
            self.assertTrue(result == "History server started successfully" or "History server already started")

    def test_stop_history_server(self):
        with patch('ae_client.AnalyticsEngineClient.stop_history_server') as mock_get:
            mock_get.return_value= ""
            result = analytics_engine.stop_history_server(INSTANCE_NAME)
            self.assertTrue(result == "")

    def test_submit_job(self):
        with self.assertRaises(Exception):  
            result = analytics_engine.submit_job()

        with self.assertRaises(Exception):
            result = analytics_engine.submit_job(volumes=[])


        volumes = [{
            "volume_name": VOLUME_NAME,
            "source_path": "",
            "mount_path": "/myapp"
            }
        ]
        size ={ 
        "num_workers": 1, 
        "worker_size": { 
            "cpu": 1, 
            "memory": "8g"
        }, 
        "driver_size": { 
            "cpu": 1, 
            "memory": "4g" 
        } 
        }
        application_jar = "/myapp/sparkify.py"
        main_class = "org.apache.spark.deploy.SparkSubmit"

        fake_json = fj.submit_job_json
        with patch('ae_client.AnalyticsEngineClient.submit_job') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.submit_job(INSTANCE_NAME,volumes=volumes, size=size, application_jar=application_jar, main_class=main_class )
            self.assertIsInstance(result, dict)
            self.assertEqual(result["id"], fake_json["id"])
            self.assertEqual(result["job_state"], fake_json["job_state"])

    def test_get_all_jobs(self):
        with self.assertRaises(Exception):  
            result = analytics_engine.get_all_jobs()

        with self.assertRaises(Exception):  
            result = analytics_engine.get_all_jobs(unique_id)
        
        with self.assertRaises(AttributeError):  
            result = analytics_engine.get_all_jobs(instance_display_name = unique_id)
        
        with self.assertRaises(AttributeError):  
            result = analytics_engine.get_all_jobs(instance_id = unique_id)
        
        with self.assertRaises(AttributeError):  
            result = analytics_engine.get_all_jobs(instance_display_name = unique_id, instance_id = unique_id)

        fake_json = fj.get_all_jobs
        with patch('ae_client.AnalyticsEngineClient.get_all_jobs') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.get_all_jobs(instance_display_name = unique_id)
            self.assertIsInstance(result, list)

    def test_delete_volume(self):

        with self.assertRaises(Exception):  
            result = analytics_engine.delete_volume()
        fake_json = fj.delete_volume_json

        with patch('ae_client.AnalyticsEngineClient.delete_volume') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.delete_volume(volume_instance_display_name = unique_id)
            self.assertEqual(result["_messageCode_"], "200")
        
        with patch('ae_client.AnalyticsEngineClient.delete_volume') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.delete_volume(volume_id = unique_id)
            self.assertEqual(result["_messageCode_"], "200")

    def test_delete_instance(self):

        with self.assertRaises(Exception):  
            result = analytics_engine.delete_instance()

        fake_json = fj.delete_instance_json

        with patch('ae_client.AnalyticsEngineClient.delete_instance') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.delete_instance(nstance_display_name = unique_id)
            self.assertEqual(result["_messageCode_"], "200")
        
        with patch('ae_client.AnalyticsEngineClient.delete_instance') as mock_get:
            mock_get.return_value= fake_json
            result = analytics_engine.delete_instance(instance_id = unique_id)
            self.assertEqual(result["_messageCode_"], "200")


if __name__ == "__main__":
    test_order = ["test_get_all_instances", "test_get_all_volumes", "test_get_all_storage_class", "test_create_volume", "test_start_volume", "test_volume_status", "test_add_file_to_volume"]
    test_order = test_order + ["test_start_history_server", "test_stop_history_server", "test_create_ae_instance", "test_submit_word_count", "test_submit_job", "test_get_all_jobs"]
    test_order = test_order + ["test_delete_volume", "test_delete_instance"]
    loader = unittest.TestLoader()
    loader.sortTestMethodsUsing = lambda x, y: test_order.index(x) - test_order.index(y)
    unittest.main(testLoader=loader, verbosity=2)
