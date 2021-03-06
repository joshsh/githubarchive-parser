import glob, os, sys, math, itertools
import time as Time
from datetime import *
from dateutil.parser import *
from dateutil.relativedelta import *

#directory for temporary files and logs
tmpDir = '../tmp/'
#directory for scratch space and parsed files
scratchDir = '../scratch/'
gzDir = scratchDir + 'githubarchive_gz/'
jsonDir = scratchDir + 'githubarchive_json/'

#output directory and file names
outDir = scratchDir + 'full/'
vertexFile = 'parsed_vertices.txt'
edgeFile = 'parsed_edges.txt'

#locations of scripts called
rubyScript = 'FixGitHubArchiveDelimiters.rb'
parseScript = 'ParseGitHubArchive.groovy'  

#system specific settings
sortMem = '2G'   #memory for sort. maximize.
threads = 2 

#start and end hours to fetch from GitHubArchive
startHour = '2014-06-01-01'
endHour = '2014-06-01-23'
#startHour = '2012-03-12-01'  #set to 'beginning' for earliest possible
#endHour = '2012-11-09-23'    #set to 'now' for last possible
#endHour = '2012-03-12-04'    #set to 'now' for last possible

#verbose output for debugging
debug = False

def chunk(a, n):
    k, m = len(a) / n, len(a) % n
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def ensureDir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def cleanTmp():
    os.system('rm -rf ' + tmpDir + '*txt')

def cleanScratch():
    os.system('rm -rf ' + gzDir + '/*gz')
    os.system('rm -rf ' + jsonDir + '/*json')


def batchJob(jobIndex):
    verticesFile = tmpDir + 'vertices_' + jobIndex + '.txt'
    edgesFile = tmpDir + 'edges_' + jobIndex + '.txt'
    jobFile = tmpDir + 'job_' + jobIndex + '.txt'
    logFile = tmpDir + 'log_' + jobIndex + '.txt'

    gzFiles = glob.glob(gzDir + '*gz')
    jsonFiles = glob.glob(jsonDir + '*json')

    files = open(jobFile,'r').read().split('\n')
    out = ''
    for f in files:
        out += jsonDir + f +'\n'
    fileHandle = open(jobFile.replace('.txt','_full.txt'),'w')
    fileHandle.write(out[:-1])
    fileHandle.close()

    for f in files:
        if gzDir + f + '.gz' not in gzFiles:
            if debug: print 'downloading ' + f + '.gz'
            cmd = 'wget -q -nc -O ' 
            cmd += gzDir + f + '.gz '
            cmd += 'http://data.githubarchive.org/' + f + '.gz'
            os.system(cmd)

        if jsonDir + f not in jsonFiles:
            if debug: print 'normalizing ' + f
            cmd = 'ruby ' + rubyScript + ' ' + gzDir + f + '.gz ' + jsonDir + f
            os.system(cmd)

    print 'parsing job ' + jobIndex
    cmd = 'groovy ' + parseScript + ' ' + jobFile.replace('.txt','_full.txt') 
    #cmd = '/mnt/vadasg/titan/bin/gremlin.sh -e ' + parseScript + ' ' + jobFile.replace('.txt','_full.txt') 
    cmd += ' ' + verticesFile + ' ' + edgesFile + ' > ' + logFile
    os.system(cmd)

def deploy():
    ensureDir(gzDir)
    ensureDir(scratchDir)
    ensureDir(jsonDir)
    ensureDir(tmpDir)
    ensureDir(outDir)


    start = Time.time()

    fileList = []

    if startHour == 'beginning':
        current = parse('2012-03-10-22')
    else:
        current = parse(startHour)

    if endHour == 'now':
        end = datetime.now()
    else:
        end =  parse(endHour)

    while current < end:
        fileList.append(current.strftime("%Y-%m-%d-X%H").replace('X0','').replace('X','') + '.json')
        current += relativedelta(hours=+1)

    jobs = min([len(fileList),threads])

    splitFileList = chunk(fileList,jobs)

    jobIndex = 0
    for s in splitFileList:
        jobFile = tmpDir + 'job_' + str(jobIndex) + '.txt'
        out = ''
        for f in s:
            out += f + '\n'
        fileHandle = open(jobFile,'w')
        fileHandle.write(out[:-1])
        fileHandle.close()
        jobIndex += 1

    bashscript  ='#!/bin/bash\nfor i in {0..' + str(jobs-1) + '}; do python ' + sys.argv[0] + ' job $i & done; wait'
    f = open('par.sh','w')
    f.write(bashscript)
    f.close()

    cmd = 'chmod +x par.sh; ./par.sh'
    if debug: print cmd
    os.system(cmd)
    
    print 'sorting output'
    cmd = 'time cat ' + tmpDir + 'vertices_*.txt | '
    cmd += 'sort -u -S' + sortMem + ' -T' + tmpDir + ' -o ' + outDir + vertexFile
    if debug: print cmd
    os.system(cmd)
    cmd = 'time cat ' + tmpDir + 'edges_*.txt | '
    cmd += 'sort -u -S' + sortMem + ' -T' + tmpDir + ' -o ' + outDir + edgeFile
    if debug: print cmd
    os.system(cmd)

    print 'done!'
    print (Time.time()-start), 'seconds elapsed'




if __name__ == '__main__':
    if 'batch' in sys.argv:
        cleanTmp()
        deploy()
    elif 'job' in sys.argv:
        jobIndex = sys.argv[2]
        batchJob(jobIndex)
    elif 'clean' in sys.argv:
        cleanTmp()
        cleanScratch()
    else:
        print """Usage:
        $ python AutomatedParallelParser.py <command>
        commands:
        clean\t\tdelete all temporary files for a fresh start
        batch\t\tstart batch processing
        job <jobIndex>\trun job (called during batch processing)
        """

