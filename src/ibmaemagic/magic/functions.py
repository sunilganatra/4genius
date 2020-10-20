"""
#
# define the jupyter notebook magic function
# ref: https://ipython.readthedocs.io/en/stable/config/custommagics.html
#
"""
from ibmaemagic.magic.analytic_magic_client import AnalyticMagicClient

from IPython.core.magic import register_line_magic, register_cell_magic
from IPython.core.display import display, HTML

from multiprocessing import Process
from tqdm.auto import tqdm
from datetime import datetime
import time
import argparse
import shlex

@register_line_magic
def analytic_engine(line):
    """
    submit codes in cell as spark job to IBM analytic engine
    @param::line: the parameter line. e.g., %%analytic_engine_start --uid xxxx --pwd xxx --host xxx
    return: none
    """
    
    ##
    # define parameter uid: the username
    parser = argparse.ArgumentParser(description='connect to the IBM analytic engine')
    parser.add_argument('--uid', dest='uid', type=str)
    
    ##
    # define parameter pwd: the passwor
    parser.add_argument('--password', dest='pwd', type=str)
    
    ##
    # define parameter host: the hostname
    parser.add_argument('--host', dest='host', type=str)
    args = parser.parse_args(shlex.split(line))
    
    ##
    # initialize AE 
    AnalyticMagicClient.init(args.host, args.uid, args.pwd)
    
    ##
    # list all instances on AE
    data = AnalyticMagicClient.get_all_instances()
    render_instance_list(data)
    
@register_cell_magic
def new_session(line, cell):
    """
    config the analytic engine session
    @param::line: the parameter line. e.g., %%new_session
    return: none
    """
    
    ##
    # initialize AE 
    AnalyticMagicClient.session = None
    AnalyticMagicClient.create_session(cell)
    if AnalyticMagicClient.session != None:
        print('> new session is created and attached to AE instance: %s'%(AnalyticMagicClient.session['name']))
    else:
        print('> create new session failed.')
    
@register_line_magic
def list_instance(line):
    data = AnalyticMagicClient.get_all_instances()
    render_instance_list(data)

@register_line_magic
def list_job(line):
    jobs = AnalyticMagicClient.list_jobs()
    print("> List of jobs: ")
    if len(jobs) == 0:
        print('**no job is running**')
    else:
        for job in jobs:
            print("job id: %s \t job state: %s"%(job['id'],job['job_state']))  

@register_cell_magic
def submit_job(line, cell):
    """
    submit codes in cell as spark job to IBM analytic engine
    @param::line: the parameter line. e.g., %%submit_job [--sync --report-progress 1
    @param::cell: the codes in the cell block
    return: none
    """
    
    ##
    # define parameter sync: whether to block cell execution
    parser = argparse.ArgumentParser(description='process parameter')
    parser.add_argument('--sync', dest='sync', action='store_true')
    parser.add_argument('--async', dest='sync', action='store_false')
    parser.set_defaults(sync=False)
    
    ##
    # define parameter report progress: progress report interval
    parser.add_argument('--report-progress', type=int, default=1)
    args = parser.parse_args(shlex.split(line))
    
    ##
    # print out submit log
    print('submit job summary:')
    print ('-------------------')
    print('> parameters:')
    print('  - synchronization:', args.sync)
    print('  - report progress every %d seconds:'%(args.report_progress))
    
    ##
    # submit job
    print('> submitting job ...')
    response = AnalyticMagicClient.submit_job(cell)
    print('> job submitted:')
    print('  - job id: %s'%(response['id']))
    print('  - job state: %s'%(response['job_state']))
    
    ##
    # check submitted job progress
    p = Process(target=heart_beat, args=(args.report_progress,))
    p.start()
    if args.sync: # whether to block the cell execution
        p.join()
        
def heart_beat(n):
    print('')
    pbar = tqdm(total=10, unit="task")
    spins = ['|','/','\\']
    spin_index = 0
    foo = [1,2,3,4,5,6,7,8,9,10]  
    for _ in foo:
        pbar.set_description('%s job execution progress:'%(spins[spin_index%3]))
        spin_index +=1
        pbar.update(1)
        time.sleep(n)

def render_instance_list(data):
    html = '<table><tr><th>Instance ID</th><th>Instance Name</th><th>Created Date</th></tr>'
    for instance in data:
        instance_id = instance['ID']
        instance_name = instance['ServiceInstanceDisplayName']
        create_date = datetime.strptime(instance['CreatedAt'].split('T')[0], "%Y-%m-%d").strftime("%Y-%m-%d")
        html += '<tr><td>%s</td><td>%s</td><td>%s</td></tr>'%(instance_id, instance_name, create_date)
    html += '</table>'
    display(HTML(html))

    

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