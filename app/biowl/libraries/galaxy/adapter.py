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

from ...fileop import IOHelper, PosixFileSystem
from ....util import Utility
from ...ssh import ssh_command

#gi = GalaxyInstance(url='http://sr-p2irc-big8.usask.ca:8080', key='7483fa940d53add053903042c39f853a')
#  r = toolClient.run_tool('a799d38679e985db', 'toolshed.g2.bx.psu.edu/repos/devteam/fastq_groomer/fastq_groomer/1.0.4', params)
srlab_galaxy = 'http://sr-p2irc-big8.usask.ca:8080'
srlab_key='7483fa940d53add053903042c39f853a'

galaxies = {}

def get_galaxy_server(*args):
    return args[0] if len(args) > 0 and args[0] is not None else srlab_galaxy

def get_galaxy_key(*args):
    return args[1] if len(args) > 1 and args[1] is not None else srlab_key

def get_galaxy_instance(server, key):
    if (server, key) in galaxies:
        return galaxies[(server, key)]
    
    gi = GalaxyInstance(server, key)
    galaxies[(server, key)] = gi
    return gi

def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())

def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)

def create_galaxy_instance(*args):
    server = get_galaxy_server(*args)
    key = get_galaxy_key(*args)
    return get_galaxy_instance(server, key)

#workflow  
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

def get_workflow_info(*args):
    gi = create_galaxy_instance(*args)
    workflow_info = gi.workflows.show_workflow(args[3])
    return workflow_info

def run_workflow(*args, **kwargs):
    gi = create_galaxy_instance(*args)
    
    if len(args) <= 3:
        raise ValueError("Parameter for workflow id is missing.")
    workflow_id = args[3]
    
    history_name = args[4] if len(args) > 4 else 'New Workflow Execution History'
    
    datamap = dict()
    for k,v in kwargs.items():
        if v:
            values = v.split("=")
            if len(values) == 2:
                datamap[k] = { 'src': str(values[0]), 'id':str(values[1]) }
                continue 
        datamap[k] = v
    
    return gi.workflows.run_workflow(workflow_id, datamap, history_name)

#library       
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

def get_library_info(*args):
    gi = create_galaxy_instance(*args)
    libraries = gi.libraries.get_libraries(library_id = args[3])
    return libraries[0] if libraries else None

def create_library(*args):
    gi = create_galaxy_instance(*args)
    name = args[3] if len(args) > 3 else str(uuid.uuid4())
    library = gi.libraries.create_library(name)
    return library["id"]

#history
def get_histories_json(*args):
    gi = create_galaxy_instance(*args)
    return gi.histories.get_histories()
    
def get_history_ids(*args):
    histories = get_histories_json(*args)
    history_ids = []
    for h in histories:
        #yield j.name
        history_ids.append(h['id'])
    return history_ids

def get_history_info(*args):
    gi = create_galaxy_instance(*args)
    histories = gi.histories.get_histories(history_id = args[3])
    return histories[0] if histories else None
  
def get_most_recent_history(*args):
    gi = create_galaxy_instance(*args)
    hi = gi.histories.get_most_recently_used_history()
    return hi['id']
        
def create_history(*args):
    gi = create_galaxy_instance(*args)
    historyName = args[3] if len(args) > 3 else str(uuid.uuid4())
    h = gi.histories.create_history(historyName)
    return h["id"]

def history_id_to_name(*args):
    info = get_history_info(*args)
    if info:
        return info['name']

def history_name_to_ids(*args):
    gi = create_galaxy_instance(*args)
    histories = gi.histories.get_histories(name = args[3])
    history_ids = []
    for history in histories:
        history_ids.append(history['id'])
    return history_ids

#tool        
def get_tools_json(*args):
    gi = create_galaxy_instance(*args)
    return gi.tools.get_tools()

def get_tool_ids(*args):
    tools = get_tools_json(*args)
    tool_ids = []
    for t in tools:
        #yield j.name
        tool_ids.append(t['id'])
    return tool_ids
   
def get_tool_info(*args):
    gi = create_galaxy_instance(*args)
    tools = gi.tools.get_tools(tool_id = args[3])
    if tools:
        return tools[0]

def tool_id_to_name(*args):
    tool = get_tool_info(*args)
    if tool:
        return tool['name']

def tool_name_to_ids(*args):
    gi = create_galaxy_instance(*args)
    tools = gi.tools.get_tools(name = args[3])
    tool_ids = []
    for t in tools:
        tool_ids.append(t['id'])
    return tool_ids

def tool_name_to_id(*args):
    tool_ids = tool_name_to_ids(*args)
    return tool_ids[0] if tool_ids else None

def get_tool_names(*args):
    tools = get_tools_json(*args)
    tool_names = []
    for t in tools:
        #yield j.name
        tool_names.append(t['name'])
    return tool_names
                          
def get_tool_params(*args):
    gi = create_galaxy_instance(*args)
    ts = gi.tools.show_tool(tool_id = args[3], io_details=True)
    return ts[args[4]]  if len(args) > 4 else ts
 
# dataset                                       
def get_history_datasets(*args):
    gi = create_galaxy_instance(*args)
    historyid = args[3] if len(args) > 3 else get_most_recent_history(gi)
    name = args[4] if len(args) > 4 else None

    datasets = gi.histories.show_matching_datasets(historyid, name)
    ids = []
    for dataset in datasets:
        ids.append(dataset['id'])
    return ids
                          
def dataset_id_to_name(*args):
    gi = create_galaxy_instance(*args)
    t = args[4] if len(args) > 4 else 'hda'
    info = gi.datasets.show_dataset(dataset_id = args[3], hda_ldda = t)
    if info:
        return info['name']

def dataset_name_to_ids(*args):
    gi = create_galaxy_instance(*args)
    h = HistoryClient(gi)
    historyid = args[4] if len(args) > 4 else get_most_recent_history(*args)
    ds_infos = h.show_matching_datasets(historyid, args[3])
    ids = []
    for info in ds_infos:
        ids.append(info['id'])
    return ids

# def upload(*args):
#     gi = create_galaxy_instance(*args)
#     library = get_library(*args)
#     if library is not None:
#         return gi.libraries.upload_file_from_local_path(library['id'], args[4])
#     else:
#         r = gi.tools.upload_file(args[4], args[3])
    
def upload_to_library_from_url(*args):
    gi = create_galaxy_instance(*args)
    d = gi.libraries.upload_file_from_url(args[4], args[3])
    return d["id"]

def http_to_local_file(remote_name, destfile):
    with urllib.request.urlopen(remote_name) as response, open(destfile, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

def wait_for_job_completion(gi, job_id):
    jc = JobsClient(gi)
    state = jc.get_state(job_id)
    while state != 'ok':
        time.sleep(0.5)
        state = jc.get_state(job_id)
    return jc.show_job(job_id)  
        
def ftp_download(u, destfile):       
    ftp = FTP(u.netloc)
    ftp.login()
    ftp.cwd(os.path.dirname(u.path))
    ftp.retrbinary("RETR " + os.path.basename(u.path), open(destfile, 'wb').write)

def fs_upload(local_file, *args, **kwargs):
    gi = create_galaxy_instance(*args)

    if 'library_id' in kwargs.keys() and kwargs['library_id'] is not None:
        return gi.libraries.upload_file_from_local_path(library_id, local_file)
    else:
        if 'history_id' in kwargs.keys() and kwargs['history_id'] is not None:
            history_id = kwargs['history_id']
        else:
            history_id = create_history(*args[:3])
            
        return gi.tools.upload_file(local_file, history_id)

def temp_file_from_urlpath(u):
    filename = os.path.basename(u.path)   
    destfile = os.path.join(tempfile.gettempdir(), filename)
    if os.path.exists(destfile):
        os.remove(destfile)
    return destfile
    
def ssh_download(src, dest):
    wget_cmd = 'wget ' + src + ' -P ' + dest
    return ssh_command('sr-p2irc-big8.usask.ca', 'phenodoop', 'sr-hadoop', wget_cmd)
    
def classic_ftp_upload(u, *args, **kwargs):
    destfile = temp_file_from_urlpath(u)
    ftp_download(u, destfile)
    fs_upload(destfile, *args, **kwargs)
        
def ftp_upload(u, *args, **kwargs):
    src = urlunparse(list(u))
    #destDir = '/home/phenodoop/galaxy_import/' + str(uuid.uuid4())
    destDir = '/hadoop/galaxy/galaxy_import/' + str(uuid.uuid4())
    status = ssh_download(src, destDir)
    if status != 0:
        raise ValueError("ssh download failed.")
#     srcFTP = FTP(u.netloc)
#     srcFTP.login()
#     srcFTP.cwd(os.path.dirname(u.path))
#     srcFTP.voidcmd('TYPE I')
#     
#     destDir = str(uuid.uuid4())
    try:
#         destFTP = FTP("sr-p2irc-big8.usask.ca", 'phenodoop', 'sr-hadoop')
#         #destFTP.login()
#         
#         destFTP.cwd("galaxy_import")
#         destFTP.mkd(destDir)
#         destFTP.cwd(destDir)
#         destFTP.voidcmd('TYPE I')
#         
#         from_Sock = srcFTP.transfercmd("RETR " + os.path.basename(u.path))
#         to_Sock = destFTP.transfercmd("STOR " + os.path.basename(u.path))
#         
#         state = 0
#         while 1:
#             block = from_Sock.recv(1024)
#             if len(block) == 0:
#                 break
#             state += len(block)
#             while len(block) > 0:
#                 sentlen = to_Sock.send(block)
#                 block = block[sentlen:]     
#         
#         from_Sock.close()
#         to_Sock.close()
#         srcFTP.quit()
#         destFTP.quit()
        gi = create_galaxy_instance(*args)
        if 'library_id' in kwargs.keys() and kwargs['library_id'] is not None:
            return gi.libraries.upload_file_from_server(kwargs['library_id'], destDir)
        else:
            # get/create a history first
            if 'history_id' in kwargs.keys() and kwargs['history_id'] is not None:
                history_id = kwargs['history_id']
            else:
                history_id = create_history(*args[:3])
            # get the import_dir library 
            libs = gi.libraries.get_libraries(name='import_dir')
            lib = libs[0] if libs else gi.libraries.create_library(name='import_dir')
            d = gi.libraries.upload_file_from_server(lib['id'], destDir)
            if d:
                dataset = gi.histories.upload_dataset_from_library(history_id, d[0]['id'])
                return dataset['id']
    except:
        return classic_ftp_upload(u, *args **kwargs)

def get_normalized_path(path):
    path = Utility.get_quota_path(path)
    fs = PosixFileSystem(Utility.get_rootdir(2))       
    return fs.normalize_path(path)
              
def local_upload(*args, **kwargs):
    u = urlparse(args[3])
        
    job = None
    if u.scheme:
        if u.scheme.lower() == 'http' or u.scheme.lower() == 'https':
            tempfile = temp_file_from_urlpath(u)
            http_to_local_file(args[3], tempfile)
            job = fs_upload(tempfile, *args, **kwargs)
            return job['outputs'][0]['id']
        elif u.scheme.lower() == 'ftp':
            return ftp_upload(u, *args, **kwargs) if get_galaxy_server(*args) == srlab_galaxy else classic_ftp_upload(u, *args, **kwargs)
        else:
            raise ValueError('No http(s) or ftp addresses given.')
    else:
        job = fs_upload(get_normalized_path(args[3]), *args, **kwargs)
        return job['outputs'][0]['id']
    
    job_info = wait_for_job_completion(gi, job['jobs'][0]['id'])
    return job_info['outputs']['output0']['id']

def upload(*args, **kwargs):    
    return local_upload(*args, **kwargs)

def local_run_tool(history_id, tool_id, inputs, *args):
    gi = create_galaxy_instance(*args)
    toolClient = ToolClient(gi)
    d = toolClient.run_tool(history_id=history_id, tool_id=tool_id, tool_inputs=inputs)
    job_info = wait_for_job_completion(gi, d['jobs'][0]['id'])
    return job_info#['outputs']['output_file']['id']

def local_run_named_tool(history_id, tool_name, inputs, *args):
    server_args = list(args)
    server_args.append(tool_name)
    tool_ids = tool_name_to_ids(*server_args)
    if not tool_ids:
        raise ValueError('Tool {0} not found.'.format(tool_name))
    return local_run_tool(history_id, tool_ids[0], inputs)
            
def run_tool(*args):
    return local_run_tool(args[3], args[4], args[5], *arg[:3])

#galaxy.datatypes.tabular:Vcf
#galaxy.datatypes.binary:TwoBit
#galaxy.datatypes.binary:Bam
#galaxy.datatypes.binary:Sff
#galaxy.datatypes.xml:Phyloxml
#galaxy.datatypes.xml:GenericXml
#galaxy.datatypes.sequence:Maf
#galaxy.datatypes.sequence:Lav
#galaxy.datatypes.sequence:csFasta
def get_datatype_local(gi, id):
    try:
        info = gi.datasets.show_dataset(dataset_id = id, hda_ldda = 'hda')
    except:
        try:
            info = gi.datasets.show_dataset(dataset_id = id, hda_ldda = 'ldda')
        except:
            return None
    return info['data_type']

def get_datatype(*args):
    gi = create_galaxy_instance(*args)
    return get_datatype_local(gi, args[3])

def get_installed_datatypes(*args):
    gi = create_galaxy_instance(*args)
    return gi.datatypes.get_datatypes()

def get_installed_sniffers(*args):
    gi = create_galaxy_instance(*args)
    return gi.datatypes.get_sniffers()

def find_dataset(*args):
    gi = create_galaxy_instance(*args)
    try:
        src = 'hda'
        info = gi.datasets.show_dataset(dataset_id = args[3], hda_ldda = src)
    except:
        try:
            src = 'ldda'
            info = gi.datasets.show_dataset(dataset_id = args[3], hda_ldda = src)
        except:
            return None, None        
    return src, info['id']

def find_or_upload_dataset(history_id, *dataargs):
    src, data_id = find_dataset(*dataargs)
    if not data_id:
        src, data_id = 'hda', upload(*dataargs, history_id = history_id)
    return src, data_id
    
def get_dataset(hda, ldda, dataname, history_id, *args, **kwargs):
    if hda in kwargs.keys():
        return 'hda', kwargs[hda]
    elif ldda in kwargs.keys():
        return 'ldda', kwargs[ldda]
    
    data = None
    dataindex = 3
    if dataname and dataname in kwargs.keys():
        data = kwargs[dataname]
    elif len(args) > dataindex:
        data = args[dataindex]
    
    if data:
        dataargs = list(args[:3])
        dataargs.append(data)
        return find_or_upload_dataset(history_id, *dataargs)
    else:
        return None, None   

def get_or_create_history(*args, **kwargs):
    if 'history_id' in kwargs.keys():
        return kwargs['history_id']
    else:
        return create_history(*args[:3])
                       
#===============================================================================
# run_fastq_groomer
# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/fastq_groomer/fastq_groomer/1.0.4",
# "tool_version":"1.0.4",
# "inputs":{"input_file":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":1,"id":"c9468fdb6dc5c5f1"}],"batch":false},
# "input_type":"sanger","options_type|options_type_selector":"basic"}}
#===============================================================================
def run_fastq_groomer(*args, **kwargs):
    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    input = {
        "input_file":{
            "values":[{
                "src":src, "id":data_id
                }]
            }
        }
    
    tool_id = 'toolshed.g2.bx.psu.edu/repos/devteam/fastq_groomer/fastq_groomer/1.0.4' #tool_name_to_id('FASTQ Groomer')
    output = local_run_tool(history_id, tool_id, input, *args[:3])
    return output['outputs']['output_file']['id']

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
def run_bwa(*args, **kwargs):
    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    refsrc, refdata_id = get_dataset('refhda', 'refldda', 'ref', history_id, *args, **datakwargs)
    if not refdata_id:
        raise ValueError("No dataset given for reference data. Give a dataset path or hda or ldda")
        
    tempargs = list(args[:3])
    data, data_id = get_dataset('hda', 'ldda', 'data', history_id, *tempargs, **datakwargs)
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    
    if data_id and not data1_id:
        data1, data1_id =  data, data_id
    
    check_arg = lambda x: x not in kwargs.keys()
    dataparam = 3
    if not data1_id:
        if check_arg('refhda') and check_arg('refldda') and check_arg('ref'):
            dataparam += 1
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset given. Give a dataset path or hda or ldda")
    
    tempargs = list(args[:3])
    input_type = "paired"
    data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
    if not data2_id:
        if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
            dataparam += 1
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
        
    input_type = "paired" if data2_id else "single"
    
    refdataset_id = refdata_id
    dataset_id1 = data1_id
    dataset_id2 = data2_id
    
    input_name1 = "input_type|bam_input1" if 'datatype' in kwargs.keys() and kwargs['datatype'] == 'bam' else "input_type|fastq_input1"
    input_name2 = "input_type|bam_input2" if 'datatype' in kwargs.keys() and kwargs['datatype'] == 'bam' else "input_type|fastq_input2"
    
    input = {
     "reference_source|reference_source_selector":"history",
     "reference_source|ref_file":{
         "values":[{
             "src":refsrc,
#              "name":"all.cDNA",
             "tags":[],
#             "keep":False,
#              "hid":2,
             "id":refdataset_id
             }],
         "batch":False
         },
        "reference_source|index_a":"auto",
         "input_type|input_type_selector":input_type,
         input_name1:{
             "values":[{
                 "src":data1,
#                 "name":"FASTQ Groomer on data 1",
                 "tags":[],
#                 "keep":False,
#                  "hid":10,
                 "id": dataset_id1
                 }],
             "batch":False
         },
     "input_type|adv_pe_options|adv_pe_options_selector":"do_not_set",
     "rg|rg_selector":"do_not_set",
     "analysis_type|analysis_type_selector":"illumina"
     }
    
    if data2_id:
        input[input_name2] = {
         "values":[{
             "src":data2,
#             "name":"FASTQ Groomer on data 1",
             "tags":[],
#             "keep":False,
#              "hid":11,
             "id": dataset_id2
             }],
         "batch":False
         }
    
    tool_id = 'toolshed.g2.bx.psu.edu/repos/devteam/bwa/bwa/0.7.15.2' #tool_name_to_id('Map with BWA')
    output = local_run_tool(history_id, tool_id, input, *args[:3])
#     {'model_class': 'Job', 
#      'outputs': {
#          'bam_output': {
#              'src': 'hda', 'id': '7b326180327c3fcc', 'uuid': 'ccfaaa0a-946a-4a51-87b0-bdf71f13f3e6'
#           }
#       }, 
#      'state': 'ok', 
#      'create_time': '2017-10-12T11:13:34.731974', 
#      'command_line': 'ln -s "/home/phenodoop/galaxy/galaxy/database/files/000/dataset_105.dat" "localref.fa" && bwa index "localref.fa" &&                 bwa aln -t "${GALAXY_SLOTS:-1}"     "localref.fa"  "/home/phenodoop/galaxy/galaxy/database/files/000/dataset_203.dat"  > first.sai &&  bwa aln -t "${GALAXY_SLOTS:-1}"     "localref.fa"  "/home/phenodoop/galaxy/galaxy/database/files/000/dataset_203.dat"  > second.sai &&  bwa sampe      "localref.fa" first.sai second.sai "/home/phenodoop/galaxy/galaxy/database/files/000/dataset_203.dat" "/home/phenodoop/galaxy/galaxy/database/files/000/dataset_203.dat"    | samtools sort -O bam -o \'/home/phenodoop/galaxy/galaxy/database/files/000/dataset_208.dat\'', 'id': 'd448712f90897b61', 
#      'inputs': {'fastq_input2': {'src': 'hda', 'id': 'dfd15528ee538abe', 'uuid': '88502312-ba9a-4f2a-bacb-05bf176e69e7'}, 
#                 'fastq_input1': {'src': 'hda', 'id': 'dfd15528ee538abe', 'uuid': '88502312-ba9a-4f2a-bacb-05bf176e69e7'}, 
#                 'ref_file': {'src': 'hda', 'id': '7ef8021ae23ac2fc', 'uuid': 'ba22f14a-18bc-410e-ab76-160b41584436'}}, 
#      'update_time': '2017-10-12T11:16:29.151400', 'tool_id': 'toolshed.g2.bx.psu.edu/repos/devteam/bwa/bwa/0.7.15.2', 'exit_code': 0, 'external_id': '429', 
#      'params': {'input_type': '{"adv_pe_options": {"__current_case__": 1, "adv_pe_options_selector": "do_not_set"}, "fastq_input2": {"values": [{"src": "hda", "id": 204}]}, "__current_case__": 0, "input_type_selector": "paired", "fastq_input1": {"values": [{"src": "hda", "id": 204}]}}', 'rg': '{"rg_selector": "do_not_set", "__current_case__": 3}', 'dbkey': '"?"', 'chromInfo': '"/home/phenodoop/galaxy/galaxy/tool-data/shared/ucsc/chrom/?.len"', 'analysis_type': '{"analysis_type_selector": "illumina", "__current_case__": 0}', 'reference_source': '{"ref_file": {"values": [{"src": "hda", "id": 205}]}, "reference_source_selector": "history", "__current_case__": 1, "index_a": "auto"}'}}
    return output['outputs']['bam_output']['id']

# {
# "tool_id":"Cut1",
# "tool_version":"1.0.2",
# "inputs": {
#     "columnList":"c1,c2",
#     "delimiter":"T",
#     "input":{
#         "values":[{
#             "src":"hda",
#             "name":"SRR034608.fastq",
#             "tags":[],
#             "keep":false,
#             "hid":39,
#             "id":"f2f5db583bb871d6"
#             }],
#          "batch":false
#     }
#  }
# }
def run_cut(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if 'columns' in kwargs.keys():
        columns = kwargs['columns']
    else:
        if check_arg('data') and check_arg('hda') and check_arg('ldda'):
            argcount += 1
        if len(args) > argcount:
            columns = args[argcount]
            argcount += 1
        else:
            raise ValueError("Invalid arugements: columns not given.")

    if 'delimiter' in kwargs.keys():
        delimiter = kwargs['delimiter']
    else:
        if check_arg('columns'):
            argcount += 1
        if len(args) > argcount:
            delimiter = args[argcount]
        else:
            delimiter = "Tab"

    inputs = {
        "columnList":columns,
        "delimiter":delimiter[:1],
        "input":{
            "values":[{
                "src":src,
                "tags":[],
#                "keep":False,
                "id":data_id
                }]
#             "batch":False
        }
     }

    tool_id = 'Cut1' #tool_name_to_id('Cut')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']
    
# {
#     "tool_id":"trimmer",
#     "tool_version":"0.0.1",
#     "inputs":{
#         "input1":{
#             "values":
#             [{
#                 "src":"hda",
#                 "name":"SRR034608.fastq",
#                 "tags":[],
#                 "keep":false,
#                 "hid":39,
#                 "id":"f2f5db583bb871d6"
#                 }],
#                   "batch":false},
#               "col":0,"start":1,"end":0,"fastq":"","ignore":["62","64","43","60","42","45","61","124","63","36","46","58","38","37","94","35"]
#               }
#               }
def run_trim(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if 'col' in kwargs.keys():
        col = kwargs['col']
    else:
        if check_arg('data') and check_arg('hda') and check_arg('ldda'):
            argcount += 1
        if len(args) > argcount:
            col = args[argcount]
        else:
            col = 0
            
    if 'start' in kwargs.keys():
        start = kwargs['start']
    else:
        if check_arg('col'):
            argcount += 1
        if len(args) > argcount:
            start = args[argcount]
            argcount += 1
        else:
            start = 1

    if 'end' in kwargs.keys():
        end = kwargs['end']
    else:
        if check_arg('start'):
            argcount += 1
        if len(args) > argcount:
            end = args[argcount]
            argcount += 1
        else:
            end = 0
            
    if 'ignore' in kwargs.keys():
        ignore = kwargs['ignore']
    else:
        if check_arg('end'):
            argcount += 1
        if len(args) > argcount:
            ignore = args[argcount]
            argcount += 1
        else:
            ignore = ""
    if ignore:
        ignore = [ord(x) for x in ignore]#.split(",")]
    
    inputs = {
        "input1":{
            "values":[{
                "src":src,
                "id":data_id
                }]
            },
        "col":col,
        "start":start,
        "end":end,
        "fastq":"",
        "ignore":ignore
    }
    
    tool_id = 'trimmer' #tool_name_to_id('Trim')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"join1","tool_version":"2.0.2",
# "inputs":{"input1":{"values":[{"src":"hda","name":"Cut on data 1","tags":[],"keep":false,"hid":45,"id":"fee08c51df578e3d"}],"batch":false},"field1":"2","input2":{"values":[{"src":"hda","name":"Cut on data 43","tags":[],"keep":false,"hid":44,"id":"1c84aa7fc4490e6d"}],"batch":false},"field2":"1","unmatched":"-u","partial":"-p","fill_empty_columns|fill_empty_columns_switch":"no_fill"}}
def run_join(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    tempargs = list(args[:3])
    dataparam = 3
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    if not data1_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset1 given. Give a dataset path or hda1 or ldda1 or data1")

    check_arg = lambda x: x not in kwargs.keys()
    
    tempargs = list(args[:3])
    data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
    if not data2_id:
        if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
            dataparam += 1
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
        if not data2_id:
            raise ValueError("No input dataset2 given. Give a dataset path or hda2 or ldda2 or data2")
        
    if 'field1' in kwargs.keys():
        field1 = kwargs['field1']
    else:
        if check_arg('hda2') and check_arg('ldda2') and check_arg('data2'):
            dataparam += 1
        if dataparam < len(args):
            field1 = args[dataparam]
        else:
            field1 = "1"
    
    if 'field2' in kwargs.keys():
        field2 = kwargs['field2']
    else:
        if check_arg('field1'):
            dataparam += 1
        if dataparam < len(args):
            field2 = args[dataparam]
        else:
            field2 = "1"
                
    inputs = {
        "input1":{
            "values":[{
                "src":data1,
                "id":data1_id
                }]
            },
        "field1":str(field1),
        "input2":{
            "values":[{
                "src":data2,
                "id":data2_id
                }]
            },
        "field2":str(field2),
        "unmatched":"", #-u",
        "partial":"", #"-p",
        "fill_empty_columns|fill_empty_columns_switch":"no_fill"
    }
    
    tool_id = 'join1' # tool_name_to_id('Join two Datasets')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']

def get_op(prefix, opindex, argcount, *args, **kwargs):
    if prefix + str(opindex) in kwargs.keys():
        return kwargs[prefix + str(opindex)]
    else:
        if len(args) > argcount:
            return args[argcount]
    
# {"tool_id":"Grouping1","tool_version":"2.1.1",
#"inputs":
#{"input1":{"values":[{"src":"hda","name":"Cut on data 43","tags":[],"keep":false,"hid":44,"id":"1c84aa7fc4490e6d"}],"batch":false},
#"groupcol":"1","ignorecase":"false","ignorelines":null,
#"operations_0|optype":"mean","operations_0|opcol":"1","operations_0|opround":"no","operations_1|optype":"mean","operations_1|opcol":"1","operations_1|opround":"yes"}}

#{"tool_id":"Grouping1","tool_version":"2.1.1","inputs":{"input1":{"values":[{"src":"hda","name":"Join two Datasets on data 44 and data 45","tags":[],"keep":false,"hid":47,"id":"13120e62d0fbb985"}],"batch":false},
#"groupcol":"1","ignorecase":"false","ignorelines":["62","64","43"],"operations_0|optype":"mean","operations_0|opcol":"1","operations_0|opround":"no"}}
def run_group(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if check_arg('data') and check_arg('hda') and check_arg('ldda'):
        argcount += 1
    if 'groupcol' in kwargs.keys():
        groupcol = int(kwargs['groupcol'])
    else:
        if len(args) > argcount:
            groupcol = int(args[argcount])
        else:
            groupcol = 1
    
    if check_arg('groupcol'):
        argcount += 1
        
    opindex = 1
    opstr = {}
    while True:
        op = get_op('op', opindex, argcount, *args, **kwargs)
        if not op:
            break
        else:
            if check_arg('op' + str(opindex)):
                argcount += 1
                
            opitems = op.split('|')
            #"operations_0|optype":"mean","operations_0|opcol":"1","operations_0|opround":"no"
            opstr["operations_{0}|optype".format(opindex - 1)] = opitems[0] if len(opitems) > 0 and opitems[0] else "mean"
            opstr["operations_{0}|opcol".format(opindex - 1)] = opitems[1] if len(opitems) > 1 and opitems[1] else "1"
            opstr["operations_{0}|opround".format(opindex - 1)] = opitems[2] if len(opitems) > 2 and opitems[2] else "no"
            
            opindex += 1
    
    ignorecase = False
    if 'ignorecase' in kwargs.keys():
        ignorecase = kwargs['ignorecase']
    
    ignorelines = ""
    if 'ignorelines' in kwargs.keys():
        ignorelines = [ord(x) for x in kwargs['ignorelines'] ]
    
    inputs = {
        "input1":{
            "values":[{
                "src":src,
                "id":data_id
                }]
            },
        "groupcol":str(groupcol),
        "ignorecase": "true" if ignorecase else "false",
        "ignorelines":ignorelines
    }
    
    for k, v in opstr.items():
        inputs[k] = v

    #tool_id = tool_name_to_id('Group') # 'Grouping1'
    output = local_run_tool(history_id, 'Grouping1', inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"sort1","tool_version":"1.0.3","inputs":{"input":{"values":[{"src":"hda","name":"Cut on data 1","tags":[],"keep":false,"hid":45,"id":"fee08c51df578e3d"}],"batch":false},
# "column":"1","style":"num","order":"DESC","column_set_0|other_column":"2","column_set_0|other_style":"gennum","column_set_0|other_order":"ASC","column_set_1|other_column":"1","column_set_1|other_style":"alpha","column_set_1|other_order":"DESC"}}
def run_sort(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if check_arg('data') and check_arg('hda') and check_arg('ldda'):
        argcount += 1
    
    opstr = {}    
    opindex = 1
    op = ""
    if 'col' in kwargs.keys():
        op = kwargs['col']
    else:
        if len(args) > argcount:
            op = args[argcount]
            argcount += 1
    if op:
        opitems = op.split('|')
        opstr['column'] = opitems[0] if len(opitems[0]) > 0 and opitems[0] else None
        opstr['style'] = opitems[1].lower() if len(opitems) > 1 and opitems[1] else "num"
        opstr['order'] = opitems[2].upper() if len(opitems) > 2 and opitems[2] else "DESC"
        
    opindex = 1                        
    while True:
        op = get_op('col', opindex + 1, argcount, *args, **kwargs)
        if not op:
            break
        else:
            if check_arg('col' + str(opindex + 1)):
                argcount += 1
                
            opitems = op.split('|')
            
            opstr['column_set_{0}|other_column'.format(opindex - 1)] = opitems[0] if len(opitems) > 0 and opitems[0] else str(opindex + 1)
            opstr['column_set_{0}|other_style'.format(opindex - 1)] = opitems[1] if len(opitems) > 1 and opitems[1] else "num"
            opstr['column_set_{0}|other_order'.format(opindex - 1)] = opitems[2] if len(opitems) > 2 and opitems[2] else "DESC"
            
            opindex += 1

    inputs = {
        "input1":{
            "values":[{
                "src":src,
                "id":data_id
                }]
            }
    }
    
    for k, v in opstr.items():
        inputs[k] = v
        
    #tool_id = tool_name_to_id('Sort') # 'sort1'
    output = local_run_tool(history_id, 'sort1', inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"Show beginning1","tool_version":"1.0.0",
#"inputs":{"lineNum":10,
#"input":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":33,"id":"bb7d1d57fc91145a"}],"batch":false}}}
def run_selectfirst(*args, **kwargs):
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if check_arg('data') and check_arg('hda') and check_arg('ldda'):
        argcount += 1
    if 'lines' in kwargs.keys():
        lines = kwargs['lines']
    else:
        if len(args) > argcount:
            lines = args[argcount]
        else:
            lines = 10

    inputs = {
        "lineNum":int(lines),
        "input":{
            "values":[{
                "src":src,
                "id":data_id
                }]
        }
     }
    
    #tool_id = tool_name_to_id('Select First') # 'Show beginning1'
    output = local_run_tool(history_id, 'Show beginning1', inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"comp1","tool_version":"1.0.2",
#"inputs":
#{"input1":{"values":[{"src":"hda","name":"Cut on data 43","tags":[],"keep":false,"hid":44,"id":"1c84aa7fc4490e6d"}],"batch":false},
#"field1":"1",
#"input2":{"values":[{"src":"hda","name":"Sort on data 45","tags":[],"keep":false,"hid":52,"id":"7e1ddb768ae0c642"}],"batch":false},
#"field2":"1","mode":"N"}}
def run_compare(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    tempargs = list(args[:3])
    dataparam = 3
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    if not data1_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset1 given. Give a dataset path or hda1 or ldda1 or data1")
    
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
            
    tempargs = list(args[:3])
    data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
    if not data2_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
        if not data2_id:
            raise ValueError("No input dataset2 given. Give a dataset path or hda2 or ldda2 or data2")
    
    if check_arg('hda2') and check_arg('ldda2') and check_arg('data2'):
        dataparam += 1
            
    if 'field1' in kwargs.keys():
        field1 = kwargs['field1']
    else:
        if dataparam < len(args):
            field1 = args[dataparam]
        else:
            field1 = 1
    
    if check_arg('field1'):
        dataparam += 1
            
    if 'field2' in kwargs.keys():
        field2 = kwargs['field2']
    else:
        if dataparam < len(args):
            field2 = args[dataparam]
        else:
            field2 = 1
    
    field1 = str(field1)
    field2 = str(field2)
    
    if check_arg('field2'):
        dataparam += 1
            
    if 'mode' in kwargs.keys():
        mode = kwargs['mode']
    else:
        if dataparam < len(args):
            mode = args[dataparam]
        else:
            mode = "N"
            
    inputs = {
        "input1":{
            "values":[{
                "src":data1,
                "id":data1_id
                }]
            },
        "field1":field1,
        "input2":{
            "values":[{
                "src":data2,
                "id":data2_id
                }]
            },
        "field2":field2,
        "mode":mode,
    }

    #tool_id = tool_name_to_id('Compare two Datasets') # 'comp1'
    output = local_run_tool(history_id, 'comp1', inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/portiahollyoak/fastuniq/fastuniq/1.1","tool_version":"1.1",
#"inputs":{"fastq_R1":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":30,"id":"b3a78854daef4a5a"}],"batch":false},
#"fastq_R2":{"values":[{"src":"hda","name":"FASTQ Groomer on data 37","tags":[],"keep":false,"hid":38,"id":"920c23ba6ef2e3da"}],"batch":false},
#"select_output_format":"f/q"}}
def run_fastuniq(*args, **kwargs):

    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    tempargs = list(args[:3])
    dataparam = 3
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    if not data1_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset1 given. Give a dataset path or hda1 or ldda1 or data1")
    
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
            
    tempargs = list(args[:3])
    data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
    if not data2_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
        if not data2_id:
            raise ValueError("No input dataset2 given. Give a dataset path or hda2 or ldda2 or data2")
    
    if check_arg('hda2') and check_arg('ldda2') and check_arg('data2'):
        dataparam += 1
            
    if 'format' in kwargs.keys():
        format = kwargs['format']
    else:
        if dataparam < len(args):
            format = args[dataparam]
        else:
            format = "q"
            
    inputs = {
        "fastq_R1":{
            "values":[{
                "src":data1,
                "id":data1_id
                }]
            },
        "fastq_R2":{
            "values":[{
                "src":data2,
                "id":data2_id
                }]
            },
        "format":format,
    }

    tool_id = 'toolshed.g2.bx.psu.edu/repos/portiahollyoak/fastuniq/fastuniq/1.1' #tool_name_to_id('FastUniq')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']

# {"tool_id":"toolshed.g2.bx.psu.edu/repos/artbio/yac_clipper/yac/2.0.1","tool_version":"2.0.1","inputs":{"input":{"values":[{"src":"hda","name":"Select first on data 33","tags":[],"keep":false,"hid":53,"id":"b735ed9e5e005602"}],"batch":false},"min":15,"max":36,"out_format":"fasta","Nmode":"accept","clip_source|clip_source_list":"prebuilt","clip_source|clip_sequence":"TGGAATTCTCGGGTGCCAAG"}}
def run_clip_adapter(*args, **kwargs):
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    check_arg = lambda x: x not in kwargs.keys()
    argcount = 3
    if check_arg('data') and check_arg('hda') and check_arg('ldda'):
        argcount += 1
        
    if 'min' in kwargs.keys():
        min = kwargs['min']
    else:
        if len(args) > argcount:
            min = args[argcount]
        else:
            min = 15
    
    if check_arg('min'):
        argcount += 1
    if 'max' in kwargs.keys():
        max = kwargs['max']
    else:
        if len(args) > argcount:
            max = args[argcount]
        else:
            max = 15
    
    nmode = "accept" if check_args('nmode') else kwargs['nmode']
    adapter = "TGGAATTCTCGGGTGCCAAG" if check_args('adapter') else kwargs['adapter']
    source = None
    if not check_args('src'):
        source = kwargs['src']
    
#    {"tool_id":"toolshed.g2.bx.psu.edu/repos/artbio/yac_clipper/yac/2.0.1","tool_version":"2.0.1","inputs":{"input":{"values":[{"src":"hda","name":"Select first on data 33","tags":[],"keep":false,"hid":53,"id":"b735ed9e5e005602"}],"batch":false},"min":15,"max":36,"out_format":"fastq","Nmode":"accept","clip_source|clip_source_list":"user","clip_source|clip_sequence":"GAATCC"}}            
    inputs = {
        "min": min,
        "max":max,
        "out_format": "fastq" if format=="q" else "fasta",
        "Nmode":nmode,
        "clip_source|clip_source_list":"prebuilt",
        "clip_source|clip_sequence":adapter,
        "input":{
            "values":[{
                "src":src,
                "id":data_id
                }]
        }
     }
    
    if source:
        inputs["clip_source|clip_source_list"] = "user"
        inputs["clip_source|clip_sequence"] = source

    tool_id = 'toolshed.g2.bx.psu.edu/repos/artbio/yac_clipper/yac/2.0.1' #tool_name_to_id('Clip adapter')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/slegras/sickle_1_33/sickle/1.33","tool_version":"1.33",
#"inputs":{"readtype|single_or_paired":"se","readtype|input_single":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":33,"id":"bb7d1d57fc91145a"}],"batch":false},
#"qual_threshold":20,"length_threshold":20,"no_five_prime":"false","trunc_n":"false"}}

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/slegras/sickle_1_33/sickle/1.33","tool_version":"1.33",
#"inputs":{"readtype|single_or_paired":"pe_sep","readtype|input_paired1":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":30,"id":"b3a78854daef4a5a"}],"batch":false},
#"readtype|input_paired2":{"values":[{"src":"hda","name":"FASTQ Groomer on data 37","tags":[],"keep":false,"hid":38,"id":"920c23ba6ef2e3da"}],"batch":false},
#"qual_threshold":20,"length_threshold":20,"no_five_prime":"true","trunc_n":"true"}}

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/slegras/sickle_1_33/sickle/1.33","tool_version":"1.33",
#"inputs":{"readtype|single_or_paired":"pe_combo","readtype|input_combo":{"values":[{"src":"hda","name":"SRR034608.fastq","tags":[],"keep":false,"hid":39,"id":"f2f5db583bb871d6"}],"batch":false},"readtype|output_n":"false","qual_threshold":20,"length_threshold":20,"no_five_prime":"true","trunc_n":"true"}}
def run_sickle(*args, **kwargs):
    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    tempargs = list(args[:3])
    dataparam = 3
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    if not data1_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset1 given. Give a dataset path or hda1 or ldda1 or data1")
    
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
    
    if 'mode' in kwargs.keys():
        mode = kwargs['mode']
    else:
        if dataparam < len(args):
            mode = args[dataparam]
        else:
            mode = "se"
    
    if mode == "pe":                
        tempargs = list(args[:3])
        data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
        if not data2_id:
            if dataparam < len(args):
                tempargs.append(args[dataparam])
                data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
                    
        if check_arg('hda2') and check_arg('ldda2') and check_arg('data2'):
            dataparam += 1
            
    if 'quality' in kwargs.keys():
        field1 = kwargs['quality']
    else:
        quality = 20
    
    if 'length' in kwargs.keys():
        length = kwargs['length']
    else:
        length = 20
           
    inputs = {
        "qual_threshold":quality,
        "length_threshold":length,
        "no_five_prime":"true",
        "trunc_n":"true",
        "readtype|single_or_paired":mode
    }
    
    if mode == "se":
        inputs["readtype|input_single"] = {
         "values":[{
             "src":data1,
             "id": data1_id
             }]
         }
    elif mode == "pe":
        inputs["readtype|input_single"] = {
         "values":[{
             "src":data1,
             "id": data1_id
             }]
         }
        inputs["readtype|input_paired2"] = {
         "values":[{
             "src":data2,
             "id": data2_id
             }]
         }
    else:
        inputs["readtype|input_combo"] = {
         "values":[{
             "src":data1,
             "id": data1_id
             }]
         }
        inputs["readtype|output_n"] = "false"
    
    tool_id = 'toolshed.g2.bx.psu.edu/repos/slegras/sickle_1_33/sickle/1.33' #tool_name_to_id('Sickle')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['output']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/fastqc/fastqc/0.70","tool_version":"0.70",
#"inputs":{"input_file":{"values":[{"src":"hda","name":"FASTQ Groomer on data 1","tags":[],"keep":false,"hid":26,"id":"d343a822bd747ee4"}],"batch":false},"contaminants":null,"limits":null}}
def run_fastqc(*args, **kwargs):    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")


    inputs = {
        "contaminants":null,
        "limits":null,
        "input_file":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            }
        }
    
    tool_id = 'toolshed.g2.bx.psu.edu/repos/devteam/fastqc/fastqc/0.70' #tool_name_to_id('FastQC')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['output_file']['id']

#{"tool_id":"Filter1","tool_version":"1.1.0","inputs":{"input":{"values":[{"src":"hda","name":"all.cDNA (as tabular)","tags":[],"keep":false,"hid":3,"id":"7ef8021ae23ac2fc"}],"batch":false},"cond":"c1=='chr22'","header_lines":0}}
def run_filter(*args, **kwargs):    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    dataparam = 3
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
    
    if 'condition' in kwargs.keys():
        condition = kwargs['condition']
    else:
        if dataparam < len(args):
            condition = args[dataparam]
        else:
            raise ValueError("No filtering condition is given.")
            
    inputs = {
        "cond":cond,
        "input":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            }
        }
    
    #tool_id = tool_name_to_id('Filter') # Filter1
    output = local_run_tool(history_id, 'Filter1', inputs, *args[:3])
    return output['outputs']['output_file']['id']

#{"tool_id":"Convert characters1","tool_version":"1.0.0","inputs":{"convert_from":"Dt","input":{"values":[{"src":"hda","name":"Filter on data 3","tags":[],"keep":false,"hid":57,"id":"4eb3d2698c4eef35"}],"batch":false},"strip":"true","condense":"true"}}
def run_convert_to_tab(*args, **kwargs):    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    dataparam = 3
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
    
    if 'delimiter' in kwargs.keys():
        delimiter = kwargs['delimiter']
    else:
        if len(args) > dataparam:
            delimiter = args[dataparam]
        else:
            delimiter = "T"
            
    if 'delimeter' == ' ':
        delimeter = 's'
    elif 'delimeter' == '.':
        delimeter = 'Dt'
    elif 'delimeter' == '.':
        delimeter = 'C'
    elif 'delimeter' == '-':
        delimeter = 'D'
    elif 'delimeter' == '_':
        delimeter = 'U'
    elif 'delimeter' == '.':
        delimeter = 'Dt'
    elif 'delimeter' == '|':
        delimeter = 'P'
    elif 'delimeter' == ':':
        delimeter = 'Co'
    elif 'delimeter' == ';':
        delimeter = 'Sc'
    else:
        delimeter = 'T'
            
    inputs = {
        "convert_from":delimiter,
        "strip":"true",
        "condense":"true",
        "input":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            }
        }
    
    #tool_id = tool_name_to_id('Filter') # Convert characters1
    output = local_run_tool(history_id, 'Convert characters1', inputs, *args[:3])
    return output['outputs']['out_file1']['id']

# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/picard/picard_FastqToSam/2.7.1.0","tool_version":"2.7.1.0","inputs":{"input_type|input_type_selector":"se","input_type|fastq":{"values":[{"src":"hda","name":"FASTQ Groomer on data 1","tags":[],"keep":false,"hid":26,"id":"d343a822bd747ee4"}],"batch":false},"quality_format":"Standard","read_group_name":"A","sample_name":"sample-a","library_name":"","platform_unit":"","platform":"","sequencing_center":"","predicted_insert_size":"","comment":"","description":"","run_date":"","min_q":0,"max_q":93,"strip_unpairied_mate_number":"false","allow_and_ignore_empty_lines":"false","validation_stringency":"LENIENT"}}
# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/picard/picard_FastqToSam/1.136.0","tool_version":"1.136.0","inputs":{"input_type|input_type_selector":"se","input_type|fastq":{"values":[{"src":"hda","name":"SP1.fq","tags":[],"keep":false,"hid":2,"id":"cff44749a1f216fe"}],"batch":false},"quality_format":"Standard","read_group_name":"A","sample_name":"sample-a","library_name":"","platform_unit":"","platform":"","sequencing_center":"","predicted_insert_size":"","comment":"","description":"","run_date":"","min_q":0,"max_q":93,"strip_unpairied_mate_number":"false","allow_and_ignore_empty_lines":"false","validation_stringency":"LENIENT"}}
# {"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/picard/picard_FastqToSam/1.136.0","tool_version":"1.136.0","inputs":{"input_type|input_type_selector":"pe","input_type|fastq":{"values":[{"src":"hda","name":"SP1.fq","tags":[],"keep":false,"hid":2,"id":"cff44749a1f216fe"}],"batch":false},"input_type|fastq2":{"values":[{"src":"hda","name":"SP1.fq","tags":[],"keep":false,"hid":2,"id":"cff44749a1f216fe"}],"batch":false},"quality_format":"Standard","read_group_name":"A","sample_name":"sample-a","library_name":"","platform_unit":"","platform":"","sequencing_center":"","predicted_insert_size":"","comment":"","description":"","run_date":"","min_q":0,"max_q":93,"strip_unpairied_mate_number":"false","allow_and_ignore_empty_lines":"false","validation_stringency":"LENIENT"}}
def run_fastq_to_sam(*args, **kwargs):    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    dataparam = 3
    check_arg = lambda x: x not in kwargs.keys()
    if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
        dataparam += 1
    
    inputs = {
        "input_type|input_type_selector":"se",
        "input_type|fastq":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            },
        "quality_format":"Standard",
        "read_group_name":"A",
        "sample_name":"sample-a",
        "library_name":"",
        "platform_unit":"",
        "platform":"",
        "sequencing_center":"",
        "predicted_insert_size":"",
        "comment":"",
        "description":"",
        "run_date":"",
        "min_q":0,
        "max_q":93,
        "strip_unpairied_mate_number":"false",
        "allow_and_ignore_empty_lines":"false",
        "validation_stringency":"LENIENT"
    }
    
    tool_id = 'toolshed.g2.bx.psu.edu/repos/devteam/picard/picard_FastqToSam/2.7.1.0' #tool_name_to_id('FastqToSam') # 
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['outFile']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/bam_to_sam/bam_to_sam/2.0.1","tool_version":"2.0.1","inputs":{"input1":{"values":[{"src":"hda","name":"FastqToSam on data 2: reads as unaligned BAM","tags":[],"keep":false,"hid":4,"id":"1587e0955a89debb"}],"batch":false},"header":"-h"}}
def run_bam_to_sam(*args, **kwargs):
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    inputs = {
        "header":"-h",
        "input1":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            }
        }
    
    tool_id = "toolshed.g2.bx.psu.edu/repos/devteam/bam_to_sam/bam_to_sam/2.0.1" #tool_name_to_id('FastQC')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['output1']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/sam_to_bam/sam_to_bam/2.1.1","tool_version":"2.1.1","inputs":{"source|index_source":"history","source|input1":{"values":[{"src":"hda","name":"BAM-to-SAM on data 4: converted SAM","tags":[],"keep":false,"hid":5,"id":"c903e9d706700fc8"}],"batch":false},"source|ref_file":{"values":[{"src":"hda","name":"Chr1.cdna","tags":[],"keep":false,"hid":3,"id":"b7c1d0811979026c"}],"batch":false}}}
def run_sam_to_bam(*args, **kwargs):
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
        
    refsrc, refdata_id = get_dataset('refhda', 'refldda', 'ref', history_id, *args, **datakwargs)
    if not refdata_id:
        raise ValueError("No dataset given for reference data. Give a dataset path or hda or ldda.")
        
    tempargs = list(args[:3])
    data, data_id = get_dataset('hda', 'ldda', 'data', history_id, *tempargs, **datakwargs)
    
    check_arg = lambda x: x not in kwargs.keys()
    dataparam = 3
    if not data_id:
        if check_arg('refhda') and check_arg('refldda') and check_arg('ref'):
            dataparam += 1
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data, data_id = find_or_upload_dataset(history_id, *tempargs)
        if not data_id:
            raise ValueError("No input dataset given. Give a dataset path or hda or ldda.")
    
    inputs = {
        "source|index_source":"history",
        "source|input1":{
            "values":[{
                "src":data,
                "id":data_id
                }]
            },
        "source|ref_file":{
            "values":[{
                "src":refsrc,
                "id":refdata_id
                }]
            }
        }

    tool_id = "toolshed.g2.bx.psu.edu/repos/devteam/sam_to_bam/sam_to_bam/2.1.1"
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']#['output1']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/sam2interval/sam2interval/1.0.1","tool_version":"1.0.1","inputs":{"input1":{"values":[{"src":"hda","name":"BAM-to-SAM on data 114: converted SAM","tags":[],"keep":false,"hid":115,"id":"c0279aab05812500"}],"batch":false},"print_all":"-p"}}
def run_sam_to_interval(*args, **kwargs):
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    src, data_id = get_dataset('hda', 'ldda', 'data', history_id, *args, **datakwargs)
    if data_id is None:
        raise ValueError("No dataset given. Give a dataset path or hda or ldda")

    inputs = {
        "print_all":"-p",
        "input1":{
            "values":[{
                "src":src, 
                "id":data_id
                }]
            }
        }
    
    tool_id = "toolshed.g2.bx.psu.edu/repos/devteam/sam2interval/sam2interval/1.0.1" #tool_name_to_id('FastQC')
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['out_file1']['id']

#{"tool_id":"toolshed.g2.bx.psu.edu/repos/devteam/join/gops_join_1/1.0.0","tool_version":"1.0.0","inputs":{"input1":{"values":[{"src":"hda","name":"Converted Interval","tags":[],"keep":false,"hid":116,"id":"e037fdb493429c2a"}],"batch":false},"input2":{"values":[{"src":"hda","name":"Converted Interval","tags":[],"keep":false,"hid":116,"id":"e037fdb493429c2a"}],"batch":false},"min":1,"fill":"none"}}
def run_join_interval(*args, **kwargs):
    
    history_id = get_or_create_history(*args, **kwargs)
    datakwargs = dict(kwargs)
    if 'history_id' in datakwargs.keys():
        del datakwargs['history_id']
    
    tempargs = list(args[:3])
    dataparam = 3
    data1, data1_id = get_dataset('hda1', 'ldda1', 'data1', history_id, *tempargs, **datakwargs)
    if not data1_id:
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data1, data1_id = find_or_upload_dataset(history_id, *tempargs)
        if not data1_id:
            raise ValueError("No input dataset1 given. Give a dataset path or hda1 or ldda1 or data1")

    check_arg = lambda x: x not in kwargs.keys()
    
    tempargs = list(args[:3])
    data2, data2_id = get_dataset('hda2', 'ldda2', 'data2', history_id, *tempargs, **datakwargs)
    if not data2_id:
        if check_arg('hda1') and check_arg('ldda1') and check_arg('data1'):
            dataparam += 1
        if dataparam < len(args):
            tempargs.append(args[dataparam])
            data2, data2_id = find_or_upload_dataset(history_id, *tempargs)
        if not data2_id:
            raise ValueError("No input dataset2 given. Give a dataset path or hda2 or ldda2 or data2")
        
    if 'min' in kwargs.keys():
        field1 = kwargs['min']
    else:
        if check_arg('hda2') and check_arg('ldda2') and check_arg('data2'):
            dataparam += 1
        if dataparam < len(args):
            field1 = args[dataparam]
        else:
            field1 = "1"
    
               
    inputs = {
        "min":str(field1),
        "fill":"none",
        "input1":{
            "values":[{
                "src":data1,
                "id":data1_id
                }]
            },
        "input2":{
            "values":[{
                "src":data2,
                "id":data2_id
                }]
            }
    }
    
    tool_id = "toolshed.g2.bx.psu.edu/repos/devteam/join/gops_join_1/1.0.0"
    output = local_run_tool(history_id, tool_id, inputs, *args[:3])
    return output['outputs']['output']['id']

def download(*args):
    gi = create_galaxy_instance(*args)
    
    dataset = gi.datasets.show_dataset(dataset_id = args[3], hda_ldda = 'hda')
    name = dataset['name']
    
    path = get_normalized_path(args[4] if len(args) > 4 else None)
    fullpath = os.path.join(path, name)
    gi.datasets.download_dataset(args[3], file_path = fullpath, use_default_filename=False, wait_for_completion=True)
    fs = PosixFileSystem(Utility.get_rootdir(2))       
    return fs.strip_root(fullpath)