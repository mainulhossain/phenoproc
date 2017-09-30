from Bio import Entrez
from urllib.error import HTTPError  # for Python 3
from fileop import IOHelper
from Bio import Cluster

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
    search_results = search_entrez(args)
    db_type = "nucleotide"
    if len(args) > 1:
           db_type = args[1]
    return_type = "fasta"
    if len(args) > 2:
        return_type = args[2]
    return_mode = "text"
    if len(args) > 3:
        ret_mode = args[3]
        
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    count = int(search_results["Count"])
    
    batch_size = 3
    filename = IOHelper.unique_filename('output/ncbi/', args[0], return_type)
    with open(filename, "w") as out_handle:
        for start in range(0, count, batch_size):
            end = min(count, start+batch_size)
            print("Going to download record %i to %i" % (start+1, end))
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
            
    return filename

def search_and_download(*args):
    pass

def cluster(*args):
    #"cyano.txt"
    with open(args[0]) as handle:
        record = Cluster.read(handle)
        genetree = record.treecluster(method='s')
        #genetree.scale()
        exptree = record.treecluster(dist='u', transpose=1)
        record.save(args[1], genetree, exptree)