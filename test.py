#!/usr/bin/python3.6

import pmdb
#import pickle
import json
import datetime


fout = 'tst3.pmem'
dateformat = '%Y-%m-%d %H:%M:%S'

n_el = 102 #40


print("######## TESTING 0 #######")

status1 = pmdb.init_pmdb(fout, n_el)

print("######## TESTING 2 #######")

print("Finished with exit code: ", status1)

# Try generating some data

jids = [i for i in range(n_el)]
#jobs = [pickle.dumps([i,3*i**2]) for i in range(n_el)]
jobs = [json.dumps([i,3*i**2]).encode('utf8') for i in range(n_el)]
jobstages = [i//3 for i in range(n_el)]
jobpaths = ['out/test/files.npy'.encode('utf8') for i in range(n_el)]
jobdatecommitteds = ['2018-04-10 12:30:20'.encode('utf8') for i in range(n_el)]
jobtagged = [0 for _ in range(n_el)]

print(jids)
print(jobs)
print(jobstages)
print(jobpaths)
print(jobdatecommitteds)
print(jobtagged)

# Now submit these

status2 = pmdb.insert(fout, n_el,
        jobid=jids,
        job=jobs,
        jobstage=jobstages,
        #jobpath=jobpaths,
        jobdatecommitted=jobdatecommitteds,
        jobtagged=jobtagged
        )

print("######## TESTING GET #######")
#
# Try fetching a job
#for i in range(n_el):
#    print(pmdb.get(fout, 'OK', n_el, i))

ix = 24

out = pmdb.get(fout, n_el, ix)
#print(out, 'vs in job', jobs[ix])
print(out)
print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout, 'n_el, 12)
#print(out, 'vs in job', jobs[12])
#print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout,  n_el, 36)
#print(out, 'vs in job', jobs[36])
#print("Converted job to: ", json.loads(out[1]))
#out = pmdb.get(fout, n_el, ix)
#print(out, 'vs in job', jobs[ix])
#print("Converted job to: ", json.loads(out[1]))


print("######## TESTING SET #######")

out = pmdb.set(fout, n_el, ix,
        job=json.dumps([12,34]).encode('utf8'),
        jobpath=b'a_special_path!' ,
        jobdatecommitted=datetime.datetime.now().strftime(dateformat).encode('utf8')
        )
print(out)
print("CHECK IF THE JOB CHANGED?")
out = pmdb.get(fout,  n_el, ix)
print(out)
print("Converted job to: ", json.loads(out[1]))


print("######## TESTING SEARCH #######")

out = pmdb.search(fout, n_el,
        #jobid=('<', 100),
        #jobstage=('<=', 8),
        jobpath=('==', 'a_special_path!'),
        jobtagged=('==', 0),
        only_first=False,
        )
print(out)

print("######## TESTING COUNT #######")

out = pmdb.count(fout)
print(out)


