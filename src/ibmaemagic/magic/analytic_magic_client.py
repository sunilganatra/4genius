import http.client
import ssl
import json
import uuid
import os
from io import StringIO
from ibmaemagic.contrib.ibm_object_storage_persistor import IBMObjectStoragePersistor


"""
#
# The static class to interact with analytic engine
# docu: https://cloud.ibm.com/apidocs/ibm-analytics-engine#introduction
# docu: https://cloud.ibm.com/docs/AnalyticsEngine?topic=AnalyticsEngine-getting-started
# docu: https://www.ibm.com/support/producthub/icpdata/docs/view/analyze/SSQNUZ_current/wsj/spark/spark-jobs.html?t=Analyze&p=analyze
# docu: https://www.ibm.com/support/producthub/icpdata/docs/view/analyze/SSQNUZ_current/wsj/spark/spark-syntax-parms-errors.html?t=Analyze&p=analyze
#
"""


class AnalyticMagicClient():
    
    token = None
    uid = None
    pwd = None
    verbose = False
    session = {}
    cos_client = None
    cos_affix = None

    
    """
    "
    " leave instance initialization empty on purpose
    "
    """
    def __init__(self):
        pass

    """
    "
    " reset the class variables
    "
    """
    @classmethod
    def reset(cls):
        cls.token = None
        cls.uid = None
        cls.pwd = None
        cls.verbose = False
        cls.session = {}
        cls.cos_client = None
        cls.cos_affix = None
    
    """
    "
    " initialize the class variables
    "
    """
    @classmethod
    def init(cls, host, uid=None, pwd=None, verbose=False):
        """
        @param::host: host url in string
        @param::uid: the user name to access IBM analytic engine
        @param::pwd: the password to access IBM analytic engine
        @param::verbose: toggle debug info
        return none
        """

        if host == None:
            raise ValueError('> The host url is required.') 

        cls.host = host
        cls.verbose = verbose
        if uid and pwd:
            cls.uid = uid
            cls.pwd = pwd
            cls.__get_access_token__()
        elif os.environ.get('USER_ACCESS_TOKEN', None) != None:
            cls.token = os.environ['USER_ACCESS_TOKEN']
        else:
             raise ValueError('The uid/pwd can not be empty.')

        # debug info
        if cls.verbose:
            print("> Init IBM Analytic Engine successfully.")
            
    """
    "
    " check if class methods are initialized properly
    "
    """
    @classmethod
    def connection_ready(cls):
        return cls.token != None
    
    """
    "
    " create session with instance id, instance name, job and history endpoint
    "
    """
    @classmethod
    def create_session(cls, config_data):
        """
        @param::instance_id: the instance id referring to analytic engine
        return none
        """
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
        
        config = None
        try:
            config = cls.__jsonify__(config_data)
        except:
            print('> create session failed: configuration data is not valid json format.')
            return

        data = cls.get_all_instances()
        session = {}
        for instance in data:
            if str(instance['ID']) == str(config['instance_id']):
                session['id'] = instance['ID']
                session['name'] = instance['ServiceInstanceDisplayName']
                connections = instance['CreateArguments']['connection-info']
                session['job_endpoint'] = connections['Spark jobs endpoint'].replace('$HOST','')
                session['history_endpoint'] = connections['History server endpoint'].replace('$HOST','')
                session['history_server_ui'] = connections['View history server'].replace('$HOST','')
                
        session['cos'] = config['cos']
        session['cluster_size'] = config['cluster_size'] if 'cluster_size' in config else None
        cls.session = session
        cls.cos_client = IBMObjectStoragePersistor(session['cos'])
        cls.cos_affix = str(uuid.uuid4())
        
        ##
        # log information
        if cls.verbose:
            print('> session info:')
            for k,v in cls.session.items():
                print('%s: %s'%(k,v))
        
    
    """
    "
    " submit code in current cell block as new job 
    "
    """
    @classmethod
    def submit_job(cls, job, application_name='4genius_spark_job'):
        """
        This method used to submit jobs to AE instance
        @param::application_name: the name of the spark application
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        ##
        # validate client readiness
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
    
        ##
        # upload file to cos
        filename = '%s_%s.py'%(application_name,cls.cos_affix)
        cls.cos_client.write(filename, job)
        
        ##
        # submit job
        payload = cls.__build_job_payload__(filename)
        job_token = cls.__get_job_auth_token__()
        
  
        headers = {
            'jwt-auth-user-payload': job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        method = cls.session['job_endpoint']
        payload = json.dumps(payload)
        response = cls.__POST__(method, payload, headers)
        return cls.__jsonify__(response)
    
    
    """
    "
    " Get a list of job on IBM Analytic Engine from API:/ae/spark/v2/jobs
    " Access Token: job access token
    "
    """
    @classmethod
    def list_jobs(cls):
        """
        list all jobs from IBM analytic engine instance
        @param::none
        return: a list of job id.
        """
        
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
        if not cls.session:
            print('> create a new session first.')
            return
        
        method = cls.session['job_endpoint']
        job_token = cls.__get_job_auth_token__()
        
        headers = {
                'jwt-auth-user-payload': job_token,
                'accept': 'application/json',
                'content-type': 'application/json'
        }
        
        data = cls.__GET__(method, headers=headers)
        return cls.__jsonify__(data)
    
    
    """
    "
    " get all AE insances from the host
    "
    """
    @classmethod
    def get_all_instances(cls):
        """
        @param:none
        @return list of instance information in json object
        """
        
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
        
        method = '/zen-data/v2/serviceInstance?type=spark'
        data = cls.__GET__(method)
        response = cls.__jsonify__(data)
        if cls.verbose:
            print('> %s'%(response['message']))
        return response['requestObj']
    
    """
    "
    " start history server
    "
    """
    @classmethod
    def toggle_history_server(cls, enable=True):
        """
        @param::enable: bool value indicate whether to start/stop history server
        return none
        """
        
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
        
        if not cls.session:
            print('> create a new session first.')
            return
        
        job_token = cls.__get_job_auth_token__()
        headers = {
            'jwt-auth-user-payload': job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        method= cls.session['history_endpoint']
        
        if enable: 
            response = cls.__POST__(method, headers=headers)
        else:
            response = cls.__DELETE__(method, headers=headers)
            
        return response
    
    """
    "
    " access history server
    "
    """
    @classmethod
    def query_history_server(cls):
        """
        @param::enable: bool value indicate whether to start/stop history server
        return none
        """
        
        if not cls.connection_ready():
            print('> connection to IBM Analytic Engine does not exist.')
            return
        
        if not cls.session:
            print('> create a new session first.')
            return
        
        job_token = cls.__get_job_auth_token__()
        headers = {
            'jwt-auth-user-payload': job_token,
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        method= cls.session['history_server_ui']
        response = cls.__POST__(method, headers=headers)
            
        return response
    
    
    """
    "
    " authenicate user by username and password and get the authentication token 
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#creating-an-iam-bearer-token
    "
    """
    @classmethod
    def __get_access_token__(cls):
        """
        @param:: none
        return none
        """

        if cls.uid == None or cls.pwd == None:
            raise ValueError('> The username and password are both required.')
        if cls.host == None:
            raise ValueError('> The ibm analytic engine host url is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            'accept': 'application/json',
            'username': cls.uid,
            'password': cls.pwd,
        }
        method = '/v1/preauth/validateAuth'
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        tmp = cls.__jsonify__(data)
        cls.token = tmp['accessToken']
        if cls.verbose:
            print('> Initialize IBM Analytic Engine Client: %s'%(tmp['message']))
            
    """
    "
    " get the job authentication token using platform token
    "
    """
    @classmethod
    def __get_job_auth_token__(cls):
        """
        @param::token: token
        @param::AE instance display name: display_name
        return jobs authentication token
        """
        
        if cls.token == None:
            raise ValueError('> The Platform access token is required.')
        

        payload = json.dumps({"serviceInstanceDisplayname": cls.session['name']})
        method = "/zen-data/v2/serviceInstance/token"
        
        data = cls.__POST__(method, payload)
        return cls.__jsonify__(data)['AccessToken']
    
    @classmethod
    def __GET__(cls, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
         
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(cls.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    @classmethod
    def __DELETE__(cls, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(cls.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("DELETE", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    @classmethod
    def __POST__(cls, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(cls.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("POST", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    @classmethod
    def __PUT__(cls, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(cls.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PUT", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    @classmethod
    def __PATCH__(cls, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              cls.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(cls.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PATCH", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
        
    
    """
    "
    " Build AE payload to run from COS instance
    " ref: https://www.ibm.com/support/producthub/icpdata/docs/view/analyze/SSQNUZ_current/wsj/spark/using-storage.html?t=Analyze&p=analyze
    "
    """
    @classmethod
    def __build_job_payload__(cls, job_filename):
        cos = cls.session['cos']
        payload = { 
                      "engine": { 
                          "type": "spark", 
                          "conf": { 
                              "spark.app.name": job_filename, 
                              "spark.hadoop.fs.cos.servicename.endpoint":cos['endpoint'], 
                              "spark.hadoop.fs.cos.servicename.secret.key":cos['secret_key'], 
                              "spark.hadoop.fs.cos.servicename.access.key":cos['access_key'] 
                              }, 
                          "env": { 
                                  "SPARK_ENV_LOADED": "1" 
                              }, 
                          "size": cls.session['cluster_size'] if 'cluster_size' in cls.session else {},
                      },
                      "application_arguments": [], 
                      "application_jar": "cos://%s.servicename/%s"%(cos['bucket'], job_filename),
                      "main_class": "org.apache.spark.deploy.SparkSubmit" 
                 }
        return payload
    
    @classmethod
    def __jsonify__(cls, dumps):
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