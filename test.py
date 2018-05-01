#!/usr/bin/python3.6

import pmdb
import pickle


fout = 'tst3.pmem'

n_el = 48


print("######## TESTING 0 #######")

status1 = pmdb.init_pmdb(fout, n_el)

print("######## TESTING 2 #######")

print("Finished with exit code: ", status1)

# Try generating some data

jids = [i for i in range(n_el)]
jobs = [pickle.dumps([i,3*i**2]) for i in range(n_el)]
jobstages = [i//3 for i in range(n_el)]
jobpaths = ['out/test/files.npy'.encode('utf8') for i in range(n_el)]
jobdatecommitteds = ['2018-04-10 12:30:20'.encode('utf8') for i in range(n_el)]

print(jids)
print(jobs)
print(jobstages)
print(jobpaths)
print(jobdatecommitteds)

# Now submit these

status2 = pmdb.insert(fout, status1, n_el,
        jids,
        jobs,
        jobstages,
        jobpaths,
        jobdatecommitteds)

print("######## TESTING GET #######")

# Try fetching a job

ix = 24

out = pmdb.get(fout, 'OK', n_el, ix)
print("Converted job to: ", pickle.loads(out[1]))
out = pmdb.get(fout, 'OK', n_el, 12)
print("Converted job to: ", pickle.loads(out[1]))
out = pmdb.get(fout, 'OK', n_el, 36)
print("Converted job to: ", pickle.loads(out[1]))
out = pmdb.get(fout, 'OK', n_el, ix)
print("Converted job to: ", pickle.loads(out[1]))

