#!/usr/bin/python3.6

import pmdb
#import pickle
import json


fout = 'tst3.pmem'

n_el = 1024


#print("######## TESTING 0 #######")
#
#status1 = pmdb.init_pmdb(fout, n_el)
#
#print("######## TESTING 2 #######")
#
#print("Finished with exit code: ", status1)
#
## Try generating some data
#
#jids = [i for i in range(n_el)]
##jobs = [pickle.dumps([i,3*i**2]) for i in range(n_el)]
#jobs = [json.dumps([i,3*i**2]).encode('utf8') for i in range(n_el)]
#jobstages = [i//3 for i in range(n_el)]
#jobpaths = ['out/test/files.npy'.encode('utf8') for i in range(n_el)]
#jobdatecommitteds = ['2018-04-10 12:30:20'.encode('utf8') for i in range(n_el)]
#
#print(jids)
#print(jobs)
#print(jobstages)
#print(jobpaths)
#print(jobdatecommitteds)
#
## Now submit these
#
#status2 = pmdb.insert(fout, status1, n_el,
#        jids,
#        jobs,
#        jobstages,
#        jobpaths,
#        jobdatecommitteds)
#
#print("######## TESTING GET #######")
##
## Try fetching a job
##for i in range(n_el):
##    print(pmdb.get(fout, 'OK', n_el, i))

ix = 24

out = pmdb.get(fout, 'OK', n_el, ix)
#print(out, 'vs in job', jobs[ix])
print(out)
print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout, 'OK', n_el, 12)
#print(out, 'vs in job', jobs[12])
#print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout, 'OK', n_el, 36)
#print(out, 'vs in job', jobs[36])
#print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout, 'OK', n_el, ix)
#print(out, 'vs in job', jobs[ix])
#print("Converted job to: ", json.loads(out[1]))


print("######## TESTING SET #######")

out = pmdb.set(fout, 'OK', n_el, ix,
        job=json.dumps([12,34]).encode('utf8'),
        jobpath=b'a_special_path!' 
        )
print(out)
print("CHECK IF THE JOB CHANGED?")
out = pmdb.get(fout, 'OK', n_el, ix)
print(out)
print("Converted job to: ", json.loads(out[1]))


print("######## TESTING SEARCH #######")

out = pmdb.search(fout, n_el,
        jobid=('>', 5),
        jobstage=('<=', 8),
        #jobpath=('==', 'a_special_path!')
        only_first=True,
        )
print(out)

