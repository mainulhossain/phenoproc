from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.libraries import LibraryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.datasets import DatasetClient
from bioblend.galaxy.jobs import JobsClient

from urllib.parse import urlparse, urlunparse
import urllib.request
import shutil
import json
import uuid
import tempfile
from ftplib import FTP
from collections import namedtuple
import os
import time

from ...fileop import IOHelper

#gi = GalaxyInstance(url='http://sr-p2irc-big8.usask.ca:8080', key='7483fa940d53add053903042c39f853a')
#  r = toolClient.run_tool('a799d38679e985db', 'toolshed.g2.bx.psu.edu/repos/devteam/fastq_groomer/fastq_groomer/1.0.4', params)

def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())

def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)

def create_galaxy_instance(*args):
    server = args[0] if len(args) > 0 and args[0] is not None else 'http://sr-p2irc-big8.usask.ca:8080'
    key = args[1] if len(args) > 1 and args[1] is not None else '7483fa940d53add053903042c39f853a'
    return GalaxyInstance(server, key)
    
def get_workflows_json(*args):
    gi = create_galaxy_instance(*args)
    return gi.workflows.get_workflows()
    
def get_workflow_ids(*args):
    wf = get_workflows_json(*args)
    wf_ids = []
    for j in wf:
        #yield j.name
        wf_ids.append(j['id'])
    return wf_ids

def get_workflow(*args):
    wf = get_workflows_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j
        
def get_libraries_json(*args):
    gi = create_galaxy_instance(*args)
    return gi.libraries.get_libraries()
    
def get_library_ids(*args):
    wf = get_libraries_json(*args)
    wf_ids = []
    for j in wf:
        #yield j.name
        wf_ids.append(j['id'])
    return wf_ids

def get_library(*args):
    wf = get_libraries_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j
        
def get_histories_json(*args):
    gi = create_galaxy_instance(*args)
    return gi.histories.get_histories()
    
def get_history_ids(*args):
    wf = get_histories_json(*args)
    wf_ids = []
    for j in wf:
        #yield j.name
        wf_ids.append(j['id'])
    return wf_ids

def get_history(*args):
    wf = get_histories_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j

def get_most_recent_history_id(gi):
    hc = HistoryClient(gi)
    hi = hc.get_most_recently_used_history()
    return hi['id']
    
def get_most_recent_history(*args):
    gi = create_galaxy_instance(*args)
    return get_most_recent_history_id(gi)
        
def create_history(*args):
    gi = create_galaxy_instance(*args)
    hc = HistoryClient(gi)
    historyName = args[3] if len(args) > 3 else str(uuid.uuid4())
    h = hc.create_history(historyName)
    return h["id"]

def history_id_to_name(*args):
    wf = get_histories_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j['name']

def history_name_to_ids(*args):
    wf = get_histories_json(*args)
    history_name = args[3].lower()
    ids = []
    for j in wf:
        if j['name'].lower() == history_name:
            ids.append(j['id'])
    return ids
        
def get_tools_json(*args):
    gi = create_galaxy_instance(*args)
    tc = ToolClient(gi)
    return tc.get_tools()

def get_tools_ids(*args):
    wf = get_tools_json(*args)
    wf_ids = []
    for j in wf:
        #yield j.name
        wf_ids.append(j['id'])
    return wf_ids

def get_tools_names(*args):
    wf = get_tools_json(*args)
    wf_ids = []
    for j in wf:
        #yield j.name
        wf_ids.append(j['name'])
    return wf_ids
    
def get_tool(*args):
    wf = get_tools_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j

def get_tools_by_name(*args):
    wf = get_tools_json(*args)
    tool_name = args[3].lower()
    named = []
    for j in wf:
        if j['name'].lower() == tool_name:
            named.append(j)
    return named

def tool_id_to_name(*args):
    wf = get_tools_json(*args)
    for j in wf:
        if j['id'] == args[3]:
            return j['name']

def tool_name_to_id(*args):
    wf = get_tools_json(*args)
    tool_name = args[3].lower()
    for j in wf:
        if j['name'].lower() == tool_name:
            return j['id']

def get_tool_params_by_tool_name(*args):
    tools = get_tools_json(*args)
    tc = ToolClient(gi)
    tool_name = args[3].lower()     
    for t in tools:
        if t['name'].lower() == tool_name:
            ts = tc.show_tool(tool_id = t['id'], io_details=True)
            if len(args) > 4:
                return ts[args[4]]
            else:
                return ts
                            
def get_tool_params(*args):
    gi = create_galaxy_instance(*args)
    tc = ToolClient(gi)
    ts = tc.show_tool(tool_id = args[3], io_details=True)
    if len(args) > 4:
        return ts[args[4]]
    else:
        return ts
                                        
def get_history_datasets(*args):
    gi = create_galaxy_instance(*args)
    historyid = args[3] if len(args) > 3 else get_most_recent_history_id(gi)
    name = args[4] if len(args) > 4 else None

    datasets = gi.histories.show_matching_datasets(historyid, name)
    ids = []
    for dataset in datasets:
        ids.append(dataset['id'])
    return ids
                          
def upload(*args):
    gi = create_galaxy_instance(*args)
    library = get_library(*args)
    if library is not None:
        return gi.libraries.upload_file_from_local_path(library['id'], args[4])
    else:
        r = gi.tools.upload_file(args[4], args[3])
    
def run_workflow(*args):
    gi = create_galaxy_instance(*args)
    workflow_id = args[3]
    datamap = dict()
    datamap['252'] = { 'src':'hda', 'id':str(args[4]) }
    return gi.workflows.run_workflow(args[3], datamap, history_name='New Workflow Execution History')

def create_library(*args):
    gi = create_galaxy_instance(*args)
    hc = LibraryClient(gi)
    historyName = args[2] if len(args) > 2 else str(uuid.uuid4())
    h = hc.create_library(historyName)
    return h["id"]

def upload_to_library_from_url(*args):
    gi = create_galaxy_instance(*args)
    hc = LibraryClient(gi)
    libraryid = args[2]
    d = hc.upload_file_from_url(libraryid, args[3])
    return d["id"]

def ftp_to_history(u, destfile):       
    ftp = FTP(u.netloc)
    ftp.login()
    ftp.cwd(os.path.dirname(u.path))
    ftp.retrbinary("RETR " + os.path.basename(u.path), open(destfile, 'wb').write)

def http_to_history(remote_name, destfile):
    with urllib.request.urlopen(remote_name) as response, open(destfile, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

def wait_for_job_completion(gi, job_id):
    jc = JobsClient(gi)
    state = jc.get_state(job_id)
    while state != 'ok':
        time.sleep(0.5)
        state = jc.get_state(job_id)
    return jc.show_job(job_id)  
        
def download_and_upload_to_galaxy(*args):
    remote_name = args[3]
    u = urlparse(remote_name)
    
    filename = os.path.basename(u.path)   
    destfile = os.path.join(tempfile.gettempdir(), filename)
    if os.path.exists(destfile):
        os.remove(destfile)
    
    if u.scheme:
        if u.scheme.lower() == 'http' or u.scheme.lower() == 'https':
            http_to_history(remote_name, destfile)
        elif u.scheme.lower() == 'ftp':
            ftp_to_history(u, destfile)
        else:
            raise 'No http(s) address given.'
    else:
        destfile = IOHelper.normaize_path(remote_name)

    gi = create_galaxy_instance(*args)
    historyid = args[4] if len(args) > 4 else get_most_recent_history_id(gi)
    d = gi.tools.upload_file(destfile, historyid) #hid: a799d38679e985db 03501d7626bd192f
    job_info = wait_for_job_completion(gi, d['jobs'][0]['id'])
    return job_info['outputs']['output0']['id']

def dataset_id_to_name(*args):
    gi = create_galaxy_instance(*args)
    dc = DatasetClient(gi)
    t = args[4] if len(args) > 4 else 'hda'
    ds_info = dc.show_dataset(dataset_id = args[3], hda_ldda = t)
    return ds_info['name']

def dataset_name_to_ids(*args):
    gi = create_galaxy_instance(*args)
    h = HistoryClient(gi)
    historyid = args[4] if len(args) > 4 else get_most_recent_history(*args)
    ds_infos = h.show_matching_datasets(historyid, args[3])
    ids = []
    for info in ds_infos:
        ids.append(info['id'])
    return ids

def run_tool(*args):
    gi = create_galaxy_instance(*args)
    toolClient = ToolClient(gi)
    #params = json2obj(args[5])
    inputs = args[5] #json.loads(args[5]) if len(args) > 5 else None
#     if params:
#         params = params.split(",")
#         for param in params:
#             param = param.split(":")
#             inputs[str(param[0]).strip()] = param[1]
            
    d = toolClient.run_tool(history_id=args[3], tool_id=args[4], tool_inputs=inputs)
    job_info = wait_for_job_completion(gi, d['jobs'][0]['id'])
    return job_info#['outputs']['output_file']['id']


#===============================================================================
# run_fastq_groomer
# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/fastq_groomer/fastq_groomer/1.0.4",
# "tool_version":"1.0.4",
# "inputs":{"input_file":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":1,"id":"c9468fdb6dc5c5f1"}],"batch":false},
# "input_type":"sanger","options_type|options_type_selector":"basic"}}
#===============================================================================
def run_fastq_groomer(*args):
    
    historyid = args[4] if len(args) > 4 else get_most_recent_history(*args)

    dataset_ids = dataset_name_to_ids(*args)
    if len(dataset_ids) == 0:
        raise "Dataset not found"

    dataset_id = dataset_ids[0]
    input = {"input_file":{"values":[{"src":"hda", "id":dataset_id}]}}

    server_args = list(args[:3])
    server_args.append('FASTQ Groomer')
    tool_id = tool_name_to_id(*server_args)
    tool_args = list(args[:3])
    tool_args.extend([historyid, tool_id, input])

    return run_tool(*tool_args)


#===============================================================================
# run_bwa
# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/bwa/bwa/0.7.15.2","tool_version":"0.7.15.2",
# "inputs":{
#     "reference_source|reference_source_selector":"history",
#     "reference_source|ref_file":{
#         "values":[{
#             "src":"hda",
#             "name":"all.cDNA",
#             "tags":[],
#             "keep":false,
#             "hid":2,
#             "id":"0d72ca01c763d02d"}],
#         "batch":false},
#         "reference_source|index_a":"auto",
#         "input_type|input_type_selector":"paired",
#         "input_type|fastq_input1":{
#             "values":[{
#                 "src":"hda",
#                 "name":"FASTQ Groomer on data 1",
#                 "tags":[],
#                 "keep":false,
#                 "hid":10,
#                 "id":"4eb81b04b33684fd"}],
#             "batch":false
#         },
#     "input_type|fastq_input2":{
#         "values":[{
#             "src":"hda",
#             "name":"FASTQ Groomer on data 1",
#             "tags":[],
#             "keep":false,
#             "hid":11,
#             "id":"5761546ab79a71f2"}],
#         "batch":false
#     },
#     "input_type|adv_pe_options|adv_pe_options_selector":"do_not_set",
#     "rg|rg_selector":"do_not_set",
#     "analysis_type|analysis_type_selector":"illumina"
#     }
#===============================================================================
def run_bwa(*args):
    
    historyid = args[4] if len(args) > 4 else get_most_recent_history(*args)

    dataset_ids = dataset_name_to_ids(*args)
    if len(dataset_ids) == 0:
        raise "Dataset not found"

    dataset_id = dataset_ids[0]
    input = {"input_file":{"values":[{"src":"hda", "id":dataset_id}]}}

    server_args = list(args[:3])
    server_args.append('FASTQ Groomer')
    tool_id = tool_name_to_id(*server_args)
    tool_args = list(args[:3])
    tool_args.extend([historyid, tool_id, input])

    return run_tool(*tool_args)