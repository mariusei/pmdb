# pmdb: Persistent Memory Database

A database where the entries can be stored on the next generation persistent memory storage media. This allows for transactions to occur with the same speeds as one would have when using volatile memory (RAM), however, the entries and changes are saved immediately. There is thus no need for saving or loading of the database, and it will persist across multiple, independent sessions.

The code is an implementation of functional linked lists based upon an example by Andy Rudoff, Intel (https://www.usenix.org/system/files/login/articles/login_summer17_07_rudoff.pdf).

It was extended to work as a database for [job_manager](https://github.com/mariusei/job_manager). This includes some ugly, hard coded fields for each list entry:

 - `jobid` -- (intended to be unique, rising from 0) integer,
 - `job` -- Encoded JSON command, up to `MAX_JOB_SIZE` characters (specified in header file),
 - `jobstage` -- integer determining at what stage a job is,
 - `savepath` -- character arrray specifying location of saved file,
 - `datecommitted` -- character array specifying the timestamp of the entry.

The `pmem_queue` by Rudoff is extended and has the following functions:

 - `push` -- adds an individual entry to the list and stores it in the persistent memory database
 - `set` -- sets parameters of an entry
 - `count` -- returns the number of entries in the persistent database
 - `show` -- displays the contents of the database
 - `search_all` -- searches, sets elements of an array to 0 or 1 (-1 if it fails) at the indicies where the contents of an entry match the conditions. The conditions and operators are specified:
     - `***_oper`: operation to check (`==`, `>`, etc)
     - `***_q`: value that has to be satisfied
     - If the operation is a null pointer the field is not checked.
     - An extra flag, `only_first` returns the first index where the conditions match and breaks the loop.


## Python extension

The Python extension can be built either via `python3.6 setup.py build_ext --inplace` for a version that will reside only source directory, or built and installed to the system with `pip install .`.


The following functions are available through `import pmdb`:

 - `init_pmdb(path_to_pmem_file, n_elements)`
    - initializes a persistent database at the given path,
 - `insert(path_to_pmem_file, status, n_elements, job_id, job, jobstage, jobpath, jobdatecommitted)`
    - receives the properties as list entries that are batch added to the persistent database,
 - `get(path_to_pmem_file, status, n_elements, index)` 
    - retrieves a list holding the contents of the entry at `index`,
 - `set(path_to_pmem_file, status, n_elements, jobid, job=None, jobstage=None, jobpath=None, jobdatecomitted=None)` 
    - sets the non-None fields to `jobid` (the index),
 - `search(path, n_elements, jobid=(,), job=(,), jobstage=(,), jobpath=(,), jobdatecommitted=(,), only_first=False)`
    - returns list of indices where the non-zero criteria, specified as `(operator, value)` are satisfied.
      - The `operator` can be `==`, `>=`, `>`, `<=`, `<` and `!=` for numeric fields (jobid, jobstage), and `==` or `!=` for character fields (`job`, `jobpath` or `jobdatecommitted`).
      - `only_first=False` means that a full search will be done on all elements. With this flag set to `True`, the first index will be returned in the output list only.

## Requirements

- `pmdk` -- must be installed, can be run even on volatile media and a Linux kernel can be built where a region of the RAM is reserved for persistent storage (however, only as long as the computer runs). 
- `Python 3.6` for use of the Python extensions.

## Notes

The size of the persistent memory file must be specified before generating the database. This value is set in calls to `pool<pmem_queue>::create(path_to_pmem_file, "queue", FILE SIZE HERE IN BYTES, create mode)`.

The maximum length of character string entry is given in the compiler flag `MAX_JOB_SIZE`.

