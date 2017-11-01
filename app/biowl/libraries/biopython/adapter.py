from Bio import Entrez
from urllib.error import HTTPError  # for Python 3
from Bio import Cluster

from ....util import Utility
from ...fileop import IOHelper, PosixFileSystem

Entrez.email = "mainulhossain@gmail.com"

def search_entrez(*args):
    #search_string = "Myb AND txid3702[ORGN] AND 0:6000[SLEN]"
    #db = "nucleotide"
    search_string = args[0]
    db_type = args[1] if len(args) > 0 else "nucleotide"
    search_handle = Entrez.esearch(db_type, term=search_string, usehistory="y", idtype="acc")
    search_results = Entrez.read(search_handle)
    search_handle.close()
    return search_results
    
def query_search_results(*args):
    search_results = args[0]
    return search_results[args[1]]

def count_search_results(*args):
    args = list(args)
    args.append("Count")
    return int(query_search_results(*args))

def search_and_download(*args):
    
    db_type = args[1] if len(args) > 1 else "nucleotide"
    return_type = args[2] if len(args) > 2 else ".fasta"
    return_mode = args[3] if len(args) > 3 else "text"
    
    search_results = search_entrez(*args)
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    count = int(search_results["Count"])
    
    batch_size = 3
    path = Utility.get_quota_path('public')
    fs = PosixFileSystem(Utility.get_rootdir(2))
    filename = IOHelper.unique_fs_name(fs, fs.normalize_path(path), args[0], return_type)
    
    with open(filename, "w") as out_handle:
        for start in range(0, count, batch_size):
            end = min(count, start+batch_size)
            print("Going to download record {0} to {1}".format(start+1, end))
            attempt = 0
            while attempt < 3:
                attempt += 1
                try:
                    fetch_handle = Entrez.efetch(db=db_type, rettype=return_type, retmode=return_mode, retstart=start, 
                                                 retmax=batch_size, webenv=webenv, query_key=query_key, idtype="acc")
                except HTTPError as err:
                    if 500 <= err.code <= 599:
                        print("Received error from server %s" % err)
                        print("Attempt %i of 3" % attempt)
                        time.sleep(15)
                    else:
                        raise
            data = fetch_handle.read()
            fetch_handle.close()
            out_handle.write(data)
            
    return fs.strip_root(filename)

def cluster(*args):
    #"cyano.txt"
    with open(args[0]) as handle:
        record = Cluster.read(handle)
        genetree = record.treecluster(method='s')
        #genetree.scale()
        exptree = record.treecluster(dist='u', transpose=1)
        record.save(args[1], genetree, exptree)