import http.client
import ssl
import json
from io import StringIO
import mimetypes
import os
from pathlib import Path

"""
"
" The client interact with Waston Analytic Engine
"
"""


class AnalyticEngineClient():
    
    def __init__(self, host, uid=None, pwd=None, token=None, verbose=True):
        """
        @param::token: authentication token in string
        @param::host: host url in string
        return catalog client instance
        """
        if host == None:
            raise Exception('The host url is required.') 
        else:
            self.host = host
        
#         if instance_display_name == None:
#             raise Exception('Analytics Engine display name is required.') 
#         else:
#             self.instance_display_name = instance_display_name
        
        if token != None:
            self.token = token
        elif uid != None and pwd !=None:
            self.__get_auth_token__(uid,pwd)
        elif os.environ.get('USER_ACCESS_TOKEN', None) != None:
            self.token = os.environ['USER_ACCESS_TOKEN']
        else:
            raise Exception('The uid/pwd and authentication token can not be empty at the same time.')
            
        
        
        # retrieve auth token
#         if token == None:
#             self.__get_auth_token__(uid,pwd)
#         else:
#             self.token = token   
            
#         if self.token != None:
#             self.__get_jobs_auth_token__(self.token, self.instance_display_name)
#         else:
#             raise Exception('Something went wrong, during getting jobs auth token') 
        # debug info
        if verbose:
            print('Initialize Cloud Pak For Data: sucessfully!')
    
    
    
    def get_all_instances(self):
        """
        return all the analytics instance details
        """
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        return self.__jsonify__(response)
    
    def get_all_volumes(self):
        """
        return all the analytics instance details
        """
        method = '/zen-data/v2/serviceInstance?type=volumes'
        response = self.__GET__(method)
        return self.__jsonify__(response)

    def get_volume_status(self, volume_id=None, volume_name=None):
        """
        return the status of the volume
        """
        if volume_id == None and volume_name == None :
            raise Exception("Both volume_name and volume_id can't be None, need atleast one.")

        method = '/zen-data/v2/serviceInstance?type=volumes'
        response = self.__GET__(method)
        response = json.loads(response)
        if len(response["requestObj"]) == 0:
            raise Exception("No volume found")
        
        for val in response["requestObj"]:
            if val["ID"] == volume_id or val["ServiceInstanceDisplayName"] == volume_name:
                result = {"status": val["ProvisionStatus"]}
                return result
        
        raise Exception("No volume found")
    
    def get_all_storage_class(self):
        """
        returns all the available storage classes in the cluster.
        """
        method = '/zen-data/v2/storageclasses'
        response = self.__GET__(method)
        try:
            result = json.loads(response)
            if "requestObj" in result and len(result["requestObj"]) >0:
                result = [val["metadata"]["name"] for val in result["requestObj"]]
                return self.__jsonify__(json.dumps(result))
            
        except:
            return self.__jsonify__(response)
        
        return self.__jsonify__(response)
    
    
    def get_instance_id(self, instance_display_name):
        """
        @param string::instance_display_name: display name on the AE instance
        returns instance id for the AE instance 
        """
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        id = None
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps({"id":id}))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name:
                    id = val["ID"]
                    return self.__jsonify__(json.dumps({"id":id}))
    
        return self.__jsonify__(json.dumps({"id":id}))
    

    def get_instance_details(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    result = val
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    
    def get_instance_resource_quota(self,instance_display_name=None, instance_id=None):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        @param int::cpu_quota: Max CPU can be used by AE instance
        @param string::memory_quota: Max Memory can be used by AE instance
        returns Spark jobs end point url
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        history_server_ui_end_point = self.get_history_server_ui_end_point(instance_display_name, instance_id)
        spark_instance_end_point = history_server_ui_end_point['history_server_ui_endpoint'].rstrip('/historyui')
        spark_instance_end_point = spark_instance_end_point.replace(self.host, "")
        response = self.__GET__(spark_instance_end_point)
        return self.__jsonify__(json.dumps(response))
    
    
    def update_instance_resource_quota(self,instance_display_name=None, instance_id=None,cpu_quota=None,memory_quota=None):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        @param int::cpu_quota: Max CPU can be used by AE instance
        @param string::memory_quota: Max Memory can be used by AE instance
        returns Spark jobs end point url
        """
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        if cpu_quota == None or memory_quota == None:
            raise Exception("cpu_quota or memory_quota can't be None, need both.")
        
        history_server_ui_end_point = self.get_history_server_ui_end_point(instance_display_name, instance_id)
        spark_instance_end_point = history_server_ui_end_point['history_server_ui_endpoint'].rstrip('/historyui')
        resource_quota_end_point = '/{}/resource_quota'.format(spark_instance_end_point.replace(self.host, ""))
        payload = {
                "cpu_quota": cpu_quota,
                "memory_quota":memory_quota
            }
        
        response = self.__PUT__(resource_quota_end_point,payloads=json.dumps(payload))
        return response

    
    def get_spark_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns Spark jobs end point url
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"spark_jobs_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    if "$HOST" in val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]:
                        result["spark_jobs_endpoint"] = val["CreateArguments"]["connection-info"]["Spark jobs endpoint"].replace("$HOST", self.host)
                    else:
                        result["spark_jobs_endpoint"] = val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    
    def get_history_server_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance spark jobs history server endpoint
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"history_server_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    
                    if "$HOST" in val["CreateArguments"]["connection-info"]["History server endpoint"]:
                        result["history_server_endpoint"] = val["CreateArguments"]["connection-info"]["History server endpoint"].replace("$HOST", self.host)
                    else:
                        result["history_server_endpoint"] = val["CreateArguments"]["connection-info"]["History server endpoint"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    
    def get_history_server_ui_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance spark jobs history UI server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"history_server_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    
                    if "$HOST" in val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]:
                        result["history_server_ui_endpoint"] = val["CreateArguments"]["connection-info"]["View history server"].replace("$HOST", self.host)
                    else:
                        result["history_server_ui_endpoint"] = val["CreateArguments"]["connection-info"]["View history server"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    def start_history_server(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance jobs history server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        history_url= self.get_history_server_end_point(instance_display_name, instance_id)
        history_url= history_url["history_server_endpoint"].replace(self.host, "")
        response = self.__POST__(history_url)
        return self.__jsonify__(json.dumps(response))
    
    def stop_history_server(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance jobs history server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        history_url= self.get_history_server_end_point(instance_display_name, instance_id)
        history_url= history_url["history_server_endpoint"].replace(self.host, "")
        response = self.__DELETE__(history_url)
        return self.__jsonify__(json.dumps(response))
    
    def submit_word_count_job(self, instance_display_name=None, instance_id=None ):
         return self.submit_job(instance_display_name, application_arguments=["/opt/ibm/spark/examples/src/main/resources/people.txt"], application="/opt/ibm/spark/examples/src/main/python/wordcount.py")
    
    def submit_job(self, instance_display_name, instance_id=None, env ={}, volumes=[], size={}, conf={}, application_arguments = [], application_jar=None, main_class=None, application=None, params_json={}   ):
        """
        This method used to submit jobs to AE instance
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        @param dict::env: set env params
                    {
                         "PYSPARK_PYTHON": "<path>",
                         "PYTHONPATH": "<path>"
                     }
        @param list::volumes: volumes details to be mounted to the instance
                    [{
                    "volume_name": "volume anme",
                    "source_path": "",
                    "mount_path": "/mount-path"
                    }]
        @param dict::size: set executors and drivers size
                { 
                  "num_workers": 1, 
                  "worker_size": { 
                      "cpu": 1, 
                      "memory": "4g"
                  }, 
                  "driver_size": { 
                      "cpu": 1, 
                      "memory": "4g" 
                  } 
              }
        @param list::application_arguments: arguments to be passed into the spark application
        @param str::application_jar: path of file to be execcuted by spark engine
        @param str::main_class: main calss module to be used
            example: org.apache.spark.deploy.SparkSubmit
        
        returns instance id for the AE instance 
        @param dict::params_json: all the params can be sent as a json, which will be directly sent in spark submit
        
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
            
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name, instance_id)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
        self.job_token = self.__get_jobs_auth_token__(self.token, instance_display_name)
        
        ###
        # comment out by kai, not used varaible
        #type = "spark"
        
        if len(params_json) != 0:
            payload =params_json
        else:
            payload = {
                "engine": {
                    "type": "spark"
                }
            }

            if env != {}:
                payload["engine"]["env"] = env

            if volumes != []:
                payload["engine"]["volumes"] = volumes

            if size != {}:
                payload["engine"]["size"] = size
                
            if conf != {}:
                payload["engine"]["conf"] = conf
                
            if application_arguments != None:
                payload["application_arguments"] = application_arguments
            if application_jar != None:
                payload["application_jar"] = application_jar
            if main_class != None:
                payload["main_class"] = main_class
            if application != None:
                payload["application"] = application
        
        
        
        headers = {
            'Authorization' : 'Bearer {}'.format(self.token),
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        
        payload = json.dumps(payload)
        response = self.__POST__(spark_jobs_endpoint,payloads=payload, headers=headers)
        return self.__jsonify__(json.dumps(response))
    
    def upload_and_submit_job(self, instance_display_name, app_volume_name, spark_job_filename, params_json={}, instance_id=None, target_directory=None):
        """
        @param string::instance_display_name: display name on the AE instance
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param dict::params_json: spark job submit payload
        @param int::instance_id: Instance ID on the AE instance
        @param string::target_directory: path with directory structure, where file to be saved
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
        
        if app_volume_name == None:
            raise Exception("app volume name display name can't be None")
        
        if spark_job_filename == None:
            raise Exception("spark_job_filename can't be None")
        
        if target_directory == None:
            target_directory = "/my-spark-apps/"
        
        app_volume_def = {
            "volume_name": app_volume_name,
            "source_path": target_directory.lstrip('/').rstrip('/'),
            "mount_path": '/'+target_directory.lstrip('/').rstrip('/')
        }
        
        if "application" not in params_json or "application_jar" not in params_json:
            params_json["application"] = "/{}/{}".format(target_directory.lstrip('/').rstrip('/'),Path(spark_job_filename).name)
        
        if "engine" not in params_json:
            params_json["engine"] = {}
        
        if "volumes" not in params_json["engine"]:
            params_json["engine"]["volumes"] = []
        
        params_json["engine"]["volumes"].append(app_volume_def)
        
        
        self.start_volume(app_volume_name)
        target_file_name = Path(spark_job_filename).name
        response = self.add_file_to_volume(app_volume_name, spark_job_filename, target_file_name, target_directory)
        print(response)
        
        job_response = self.submit_job(instance_display_name, params_json=params_json)
        print(job_response)
    
    
    def get_all_jobs(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
        
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name, instance_id)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
#         spark_jobs_endpoint = spark_jobs_endpoint.lstrip("/")
        self.job_token = self.__get_jobs_auth_token__(self.token, instance_display_name)
        headers = {
            'jwt-auth-user-payload': self.job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        response = self.__GET__(spark_jobs_endpoint, headers=headers)
        return self.__jsonify__(json.dumps(response))
        
        
    def create_instance(self, instance_display_name,pre_existing_owner=False,transient_fields= {},service_instance_version = "3.0.1",create_arguments={} ):
        """
        @param string::instance_display_name: display name for the instance
        @param bool::pre_existing_owner: Set pre existing owner
        @param string::service_instance_version: set service instance version
        @param dict::transient_fields: dictionay to set transient fields, default is {}
        @param bool::pre_existing_owner: Set pre existing owner
        @param dict::create_arguments: set arguments for the volumes sample dictionary
            {
                "metadata":{
                   "volumeName":"volume name- must be created before",
                   "storageClass": "",
                   "storageSize": ""
                },
                "serviceInstanceDescription": "Description"
             }
        returns AE instance jobs history server
        """
        
        service_instance_type = "spark"
        
        if instance_display_name == None or len(instance_display_name) == 0:
            raise Exception("Instance display name can't be blank.")
        
        sample_creat_arguments = """
            {
                "metadata":{
                   "volumeName":"volume name- must be created before",
                   "storageClass": "",
                   "storageSize": ""
                },
                "serviceInstanceDescription": "Description"
             }"""
        
        if len(create_arguments) == 0:
            raise Exception("create_arguments dictionay can't be empty. Follow the sample: \n {}".format(sample_creat_arguments))
            
        if "metadata" not in create_arguments:
            raise Exception("create_arguments dictionay must have meta data. Follow the sample: \n {}".format(sample_creat_arguments))
        
        if "resources" not in create_arguments:
            create_arguments["resources"] = {}
        
        ##
        # dheerag, please fix problem here, volume_instance_display_name is not definied
        #
        # if "serviceInstanceDescription" not in create_arguments:
        #    create_arguments["serviceInstanceDescription"] = volume_instance_display_name
        
        payload = {
            "createArguments": create_arguments,
            "preExistingOwner": pre_existing_owner,
            "serviceInstanceDisplayName": instance_display_name,
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
            "transientFields": transient_fields
        }
        
        payload = json.dumps(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__POST__(method, payload)
        return self.__jsonify__(json.dumps(response))
     
        
    
    def create_volume(self, volume_instance_display_name,pre_existing_owner=False,transient_fields= {},service_instance_version = "-",create_arguments={} ):
        """
        @param string::volume_instance_display_name: display name for the volume
        @param bool::pre_existing_owner: Set pre existing owner
        @param string::service_instance_version: set service instance version
        @param dict::transient_fields: dictionay to set transient fields, default is {}
        @param bool::pre_existing_owner: Set pre existing owner
        @param dict::create_arguments: set arguments for the volumes sample dictionary
            {
                "metadata": {
                    "storageClass": "ibmc-file-gold-gid",
                    "storageSize": "20Gi"
                },
                "resources": {},
                "serviceInstanceDescription": "volume 1"
            }
        returns AE instance jobs history server
        """
        
        service_instance_type = "volumes"
        
        if volume_instance_display_name == None or len(volume_instance_display_name) == 0:
            raise Exception("volume instance display name can't be blank.")
        
        sample_creat_arguments = """
            {
                "metadata": {
                    "storageClass": "ibmc-file-gold-gid",
                    "storageSize": "20Gi"
                },
                "resources": {},
                "serviceInstanceDescription": "volume 1"
            }"""
        
        if len(create_arguments) == 0:
            raise Exception("create_arguments dictionay can't be empty. Follow the sample: \n {}".format(sample_creat_arguments))
            
        if "metadata" not in create_arguments:
            raise Exception("create_arguments dictionay must have meta data. Follow the sample: \n {}".format(sample_creat_arguments))
        
        if "resources" not in create_arguments:
            create_arguments["resources"] = {}
        
        if "serviceInstanceDescription" not in create_arguments:
            create_arguments["serviceInstanceDescription"] = volume_instance_display_name
        
        payload = {
            "createArguments": create_arguments,
            "preExistingOwner": pre_existing_owner,
            "serviceInstanceDisplayName": volume_instance_display_name,
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
            "transientFields": transient_fields
        }
        
        payload = json.dumps(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__POST__(method, payload)
        return self.__jsonify__(json.dumps(response))
    
    def get_spark_job_status(self, instance_display_name=None, instance_id=None, job_id=None):
        """
        @param string::instance_display_name: display name for the volume
        @param string::instance_id: Volume unique id
        @param string::job_id: spark job id
        """
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        if job_id == None:
            raise Exception("job_id can't be None.")
        
        job_end_point = self.get_spark_end_point(instance_display_name, instance_id)['spark_jobs_endpoint']
        method = '{}/{}'.format(job_end_point.replace(self.host, ""), job_id)
        response = self.__GET__(method)
        return self.__jsonify__(json.dumps(response))
    
    def delete_spark_job(self, instance_display_name=None, instance_id=None, job_id=None):
        """
        @param string::instance_display_name: display name for the volume
        @param string::instance_id: Volume unique id
        @param string::job_id: spark job id
        """
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        if job_id == None:
            raise Exception("job_id can't be None.")
        
        job_end_point = self.get_spark_end_point(instance_display_name, instance_id)['spark_jobs_endpoint']
        method = '{}/{}'.format(job_end_point.replace(self.host, ""), job_id)
        response = self.__DELETE__(method)
        return self.__jsonify__(json.dumps(response))
    
    def delete_all_finished_spark_job(self, instance_display_name=None, instance_id=None):
        """
        @param string::instance_display_name: display name for the volume
        @param string::instance_id: Volume unique id
        """
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        job_list = json.loads(self.get_all_jobs(instance_display_name, instance_id))
        
        for job in job_list:
            if job["job_state"] == "FINISHED":
                self.delete_spark_job(instance_display_name, instance_id, job_id=job["id"])
            
    def delete_all_spark_job(self, instance_display_name=None, instance_id=None):
        """
        @param string::instance_display_name: display name for the volume
        @param string::instance_id: Volume unique id
        """
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        job_list = json.loads(self.get_all_jobs(instance_display_name, instance_id))
        
        for job in job_list:
            self.delete_spark_job(instance_display_name, instance_id, job_id=job["id"])

    def delete_instance(self, instance_display_name=None, instance_id=None, service_instance_version = "-" ):
        """
        @param string::instance_display_name: display name for the volume
        @param string::instance_id: Volume unique id
        @param string::service_instance_version: set service instance version. Default : "-"  
        """
        
        service_instance_type = "spark"
        
        if instance_display_name == None and instance_id == None:
            raise Exception("Both instance_display_name and instance_id can't be blank")
        
        payload = {
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
        }
        
        if instance_id:
            payload["serviceInstanceId"] = instance_id
        if instance_display_name:
            payload["serviceInstanceDisplayName"] = instance_display_name
            
        payload = json.dumps(payload)
        print(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__DELETE__(method, payloads = payload)
        return self.__jsonify__(json.dumps(response))
    
    
    def delete_volume(self, volume_instance_display_name=None, volume_id=None, service_instance_version = "-" ):
        """
        @param string::volume_instance_display_name: display name for the volume
        @param string::volume_id: Volume unique id
        @param string::service_instance_version: set service instance version. Default : "-"  
        """
        
        service_instance_type = "volumes"
        
        if volume_instance_display_name == None and volume_id == None:
            raise Exception("Both volume_instance_display_name and volume_id can't be blank")
        
        payload = {
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
        }
        
        if volume_id:
            payload["serviceInstanceId"] = volume_id
        if volume_instance_display_name:
            payload["serviceInstanceDisplayName"] = volume_instance_display_name
            
        payload = json.dumps(payload)
        print(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__DELETE__(method, payloads = payload)
        return self.__jsonify__(json.dumps(response))

    def start_volume(self, volume_name):
        """
        @param string::volume_name: volume display name
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        method= "/zen-data/v1/volumes/volume_services/{}".format(volume_name)
        payload = json.dumps({})
        response = self.__POST__(method, payloads=payload)
        return self.__jsonify__(json.dumps(response))
    
    def download_logs(self, instance_display_name, volume_name, job_id):
        """
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param string::target_file_name: name of the file to be saved on colume
        @param string::target_directory: path with directory structure, where file to be saved
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        if job_id == None:
            raise Exception("Job id can not be empty.")
        
        if instance_display_name == None:
            raise Exception("Instance display name can not be empty.")
            
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
        instance_id = spark_jobs_endpoint.split("/")[-3]
        method = "/zen-volumes/{}/v1/volumes/files/{}%2F{}%2Flogs%2Fspark-driver-{}-stdout".format(volume_name, instance_id, job_id, job_id)
        
        ###
        # comment out by kai, not used varaible
        #
        # conn = http.client.HTTPSConnection("cp4d-cpd-cp4d.pathfinder-royalbankofc-73aebe06726e634c608c4167edcc2aeb-0000.tor01.containers.appdomain.cloud")
        # payload = ''
        
        headers = {
          'Authorization': 'Bearer {}'.format(self.token),
        }
        response = self.__GET__(method, headers=headers)
        return self.__jsonify__(json.dumps(response))
    
    def get_file_from_volume(self, volume_name, source_file , target_file_name, target_directory= None):
        """
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param string::target_file_name: name of the file to be saved on colume
        @param string::target_directory: path with directory structure, where file to be saved
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        if source_file == None:
            raise Exception("source_file param cannot be empty.")
        
        if target_file_name == None:
            raise Exception("target_file_name param cannot be empty.")
        
        if target_directory != None:
            target_directory =  target_directory.lstrip("/")#.rstrip("/")
            target_directory =  target_directory.split("/")
            target_directory = "%2F".join(target_directory)
            method = "/zen-volumes/{}/v1/volumes/files/{}{}".format(volume_name, target_directory, target_file_name)
        else:
            method = "/zen-volumes/{}/v1/volumes/files/{}".format(volume_name, target_file_name)
        headers = {
          'Authorization': 'Bearer {}'.format(self.token),
        }
        response = self.__GET__(method, headers=headers)
        return self.__jsonify__(json.dumps(response))
    
    def add_file_to_volume(self, volume_name, source_file , target_file_name, target_directory= None):
        """
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param string::target_file_name: name of the file to be saved on colume
        @param string::target_directory: path with directory structure, where file to be saved
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        if source_file == None:
            raise Exception("source_file param cannot be empty.")
        
        if target_file_name == None:
            raise Exception("target_file_name param cannot be empty.")
            
        
        conn = http.client.HTTPSConnection(self.host,context = ssl._create_unverified_context())
        dataList = []
        boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
        dataList.append('--' + boundary)
        dataList.append('Content-Disposition: form-data; name=upFile; filename={0}'.format(target_file_name))

        fileType = mimetypes.guess_type(source_file)[0] or 'application/octet-stream'
        dataList.append('Content-Type: {}'.format(fileType))
        dataList.append('')

        with open(source_file) as f:
          dataList.append(f.read())
        dataList.append('--'+boundary+'--')
        dataList.append('')
        body = '\r\n'.join(dataList)
        payload = body
        headers = {
          'Authorization': 'Bearer {}'.format(self.token),
          'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
        }
        
        if target_directory != None:
            target_directory =  target_directory.lstrip("/").rstrip("/")
            target_directory = target_directory+'/'
            target_directory =  target_directory.split("/")
            target_directory = "%2F".join(target_directory)
            method = "/zen-volumes/{}/v1/volumes/files/{}{}".format(volume_name, target_directory, target_file_name)
        else:
            method = "/zen-volumes/{}/v1/volumes/files/{}".format(volume_name, target_file_name)
        
        conn.request("PUT", method, payload, headers)
        res = conn.getresponse()
        result = res.read()
        
        try:
            result = json.loads(result)
        except:
            result = self.__jsonify__(json.dumps(result))
            
        if "_messageCode_" in result:
            if result["_messageCode_"] == "success":
                result["file_path"] = method#"/zen-volumes/{}/v1/volumes/files/{}/{}".format(volume_name, target_directory, target_file_name)
            else:
                self.__jsonify__(json.dumps(result))
                
        
        return self.__jsonify__(json.dumps(result))
    
    
    
    """
    "
    " authenicate user by username and password and get the authentication token 
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#creating-an-iam-bearer-token
    "
    """
    def __get_auth_token__(self, uid, pwd, verbose=False):
        """
        @param::uid: username
        @param::pwd: password
        return authentication token
        """
        if uid == None or pwd == None:
            raise Exception('the username and password are both required.')
        
        
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            'password':pwd,
            'username':uid
        }
        method = '/v1/preauth/validateAuth'
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        self.token = self.__jsonify__(data)['accessToken']
        return self.token
    
    def __get_jobs_auth_token__(self, token, display_name, verbose=False):
        """
        @param::token: token
        @param::AE instance display name: display_name
        return jobs authentication token
        """
        if token == None:
            raise Exception('Platform token is required.')
        
        
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            'Authorization': 'Bearer {}'.format(token)
        }
        payload = json.dumps({"serviceInstanceDisplayname": display_name})
        
        method = "/zen-data/v2/serviceInstance/token"
        conn.request("POST", method, headers=headers, body= payload)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        self.job_token = self.__jsonify__(data)['AccessToken']
        return self.job_token
            
        
    
    def __GET__(self, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    
    def __POST__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
        conn.request("POST", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PUT__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PUT", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PATCH__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PATCH", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __DELETE__(self, method, headers=None, payloads=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("DELETE", method, payloads, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
        
    
    def __jsonify__(self, dumps):
        """
        @param::dumps: json dumps in string
        return json object
        """
        dumps_io = StringIO(dumps)
        return json.load(dumps_io)




#         ┌─┐       ┌─┐
#      ┌──┘ ┴───────┘ ┴──┐
#      │                 │
#      │       ───       │
#      │  ─┬┘       └┬─  │
#      │                 │
#      │       ─┴─       │
#      │                 │
#      └───┐         ┌───┘
#          │         │
#          │         │
#          │         │
#          │         └──────────────┐
#          │                        │
#          │                        ├─┐
#          │                        ┌─┘
#          │                        │
#          └─┐  ┐  ┌───────┬──┐  ┌──┘
#            │ ─┤ ─┤       │ ─┤ ─┤
#            └──┴──┘       └──┴──┘
#                 BLESSING FROM 
#           THE BUG-FREE MIGHTY BEAST
