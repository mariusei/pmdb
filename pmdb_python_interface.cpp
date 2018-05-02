/*
 * Copyright 2015-2017, Intel Corporation
 * Copyright 2018, Marius Berge Eide
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "pmdb_core.hpp"

using pmem::obj::pool;

///////   PYTHON EXTENSION   ////////

bool init_pop(char* path, pool<pmem_queue> *pop)
{
  // Database storage
  if (file_exists(path) != 0) {
    pop[0] = pool<pmem_queue>::create(
        //path, "queue", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
        path, "queue", 16*1024*1024, CREATE_MODE_RW);
    return true;
  } else {
      pop[0] = pool<pmem_queue>::open(path, "queue");
      return false;
  }
}

PyObject* init_pmdb(PyObject *self, PyObject* args)
{
  // Input/save file name for database

  std::cout << "init_pmdb" << std::endl;

  PyObject *path_in_py;
  char* path_in;
  int64_t n_objects_in, n_objects_found;
  char* res_back;
  PyObject *path_back_py;

  pool<pmem_queue> pop;

  if (!PyArg_ParseTuple(args, "sl", 
                         &path_in,
                         &n_objects_in
                         )) {
    res_back = "STATUS_FAILED_INTERPRETATION";
    path_back_py = Py_BuildValue("s", res_back);
    return path_back_py;
  }

  std::cout << "Path in was: " << path_in << std::endl;
  std::cout << "Will now check if n elements in exist: " << n_objects_in << std::endl;

  if(init_pop(path_in, &pop)) {
    res_back = "STATUS_EMPTY"; 
  } else {
    res_back = "STATUS_OPEN_POTENTIALLY_FILLED";
  }


  // Now check the number of elements
  n_objects_found = 0;

  std::cout << "counting objects in list... from " << n_objects_found << std::endl;

  auto q = pop.get_root(); 

  n_objects_found = q->count();

  std::cout << "found n objects: " << n_objects_found << " in total" << std::endl;

  if (n_objects_found == 0) {
    res_back = "STATUS_EMPTY";
  } else if (n_objects_found < n_objects_in) {
    res_back = "STATUS_NOT_FULL";
  } else {
    res_back = "STATUS_FULL";
  }

  // NOW: return a string!
  //
  path_back_py = Py_BuildValue("s", res_back);

  pop.close();
  
  return path_back_py;

error:
  pop.close();
  return Py_BuildValue("");

}

PyObject* insert(PyObject *self, PyObject *args)
{
  /* Inserts a single element into the database.
     Must be called sequentially until all n objects
     have been inserted.
  */ 

  std::cout << "pmdb::insert" << std::endl;

  // Input
  char* path_in, status_in;
  int64_t n_max;
  // Will be inserted in DB:
  //int64_t jobid;
  //char* job;
  //int64_t jobstage;
  //char* jobpath;
  //char* jobdatecommitted;
  
  // Accessing the elements of the Python lists instead
  PyObject *jobid, *job, *jobstage, *jobpath, *jobdatecommitted;

  char* status_out;
  PyObject *status_out_py;

  std::cout << "\t Initialized " << std::endl;

  if (!PyArg_ParseTuple(args, "sslOOOOO", 
                         &path_in,
                         &status_in,
                         &n_max,
                         &jobid,
                         &job,
                         &jobstage,
                         &jobpath,
                         &jobdatecommitted
                         )) {
    status_out = "STATUS_FAILED_INTERPRETATION";
  }

  int64_t n_objects_found;

  std::cout << "\t Parsed input tuples " << std::endl;

  pool<pmem_queue> pop;
  if(init_pop(path_in, &pop)) {
    status_out = "STATUS_EMPTY"; 
  } else {
    status_out = "STATUS_OPEN_POTENTIALLY_FILLED";
  }

  std::cout << "\t Opened PM file " << std::endl;

  if (status_out == "STATUS_EMPTY") {
    // The database should have been initialized
    status_out = "STATUS_FAILED_NOT_INITIALIZED";
    status_out_py = Py_BuildValue("s", status_out);
    return status_out_py;
  }

  std::cout << "\t Checked status of file " << std::endl;


  auto q = pop.get_root();

  std::cout << "\t Found root node " << std::endl;

  n_objects_found = q->count();

  std::cout << "\t Found n objects " << n_objects_found << std::endl;

  // reset q
  q = pop.get_root();

  // Insert all at once
  if (n_objects_found < n_max) {
    // All is set, push to list
    for (int i = 0; i < n_max; i++) {
      q->push(pop, 
              PyLong_AsLong(PyList_GetItem(jobid, i)),
              PyBytes_AsString(PyList_GetItem(job, i)),
              PyLong_AsLong(PyList_GetItem(jobstage, i)),
              PyBytes_AsString(PyList_GetItem(jobpath, i)),
              PyBytes_AsString(PyList_GetItem(jobdatecommitted, i))
          );
    }
    status_out = "STATUS_INSERTED";
  } else {
    status_out = "STATUS_FAILED_TOO_MANY";
  }

  pop.close();

  std::cout << "\t Pushed data to list " << std::endl;

  status_out_py = Py_BuildValue("s", status_out);

  std::cout << "\t Done with code " << status_out << std::endl;

  return status_out_py;
  
error:
  pop.close();
  return Py_BuildValue("");
}

PyObject* get(PyObject *self, PyObject *args)
{
  /* Gets a single object,
   * returns it as a Python list 
  */ 

  std::cout << "pmdb::get" << std::endl;

  // Input
  char* path_in, status_in;
  int64_t n_max, index;

  
  // These objects will be set and stored in a list
  PyObject *jobid, *job, *jobstage, *jobpath, *jobdatecommitted;
  PyObject *outlist = PyList_New(5);

  char* status_out;

  // Parse inputs
  if (!PyArg_ParseTuple(args, "ssll", 
                         &path_in,
                         &status_in,
                         &n_max,
                         &index
                         )) {
    return Py_BuildValue("");
  //  status_out = "STATUS_FAILED_INTERPRETATION";
  }


  pool<pmem_queue> pop;
  if(init_pop(path_in, &pop)) {
    status_out = "STATUS_EMPTY"; 
  } else {
    status_out = "STATUS_OPEN_POTENTIALLY_FILLED";
  }

  std::cout << "\t Opened PM file " << std::endl;

  // reset q
  auto q = pop.get_root();

  // Retrieved object is a struct
  entry_shared data = q->get(index);

  std::cout << "\t Retrieved data, e.g. jobix " << data.jobid << " jobstage " << data.jobstage << " and job " << data.job << " ok." << std::endl;

  // whose elements can be converted
  // to populate our list
  PyList_SetItem(outlist, 0, PyLong_FromLong(data.jobid));
  PyList_SetItem(outlist, 1, PyBytes_FromString(data.job));
  PyList_SetItem(outlist, 2, PyLong_FromLong(data.jobstage));
  PyList_SetItem(outlist, 3, PyBytes_FromString(data.savepath));
  PyList_SetItem(outlist, 4, PyBytes_FromString(data.datecommitted));
  pop.close();

  std::cout << "\t Converted data" << std::endl;


  return outlist;

}

PyObject* set_pyerr(const char* errstring, PyObject* element=NULL)
{
  /* Raises exception with error string `errstring`,
   * of type `exception` (e.g. PyValueError) and
   * optionally returns a Python object `element`.
   */
  PyObject* out = PyList_New(2);
  PyObject* out1 = PyBytes_FromString(errstring);

  PyList_SetItem(out, 0, out1 );
  if (element) {
    PyList_SetItem(out, 1, element);
  } else {
    PyObject* out2 = PyLong_FromLong(0);
    PyList_SetItem(out, 1, out2);
  }

  return out;
}



PyObject* set(PyObject *self, PyObject *args, PyObject *kwords)
{
  /* Sets values to a single entry in the database
   * returns status code
  */ 

  std::cout << "pmdb::set" << std::endl;

  // Input
  char* path_in, status_in;
  int64_t n_max, index;

  
  // Potential input objects, read in as keywords
  PyObject *jobid = nullptr; // this one must be set
  PyObject *job = nullptr;
  PyObject *jobstage = nullptr;
  PyObject *jobpath = nullptr;
  PyObject *jobdatecommitted = nullptr;

  static char *kwlist[] = {"path_in", "status_in", "n_max",
    "jobid", "job", "jobstage", "jobpath", "jobdatecommitted", NULL};

  // Will be parsed into these:
  int64_t jobid_loc;
  char* job_loc;
  int64_t jobstage_loc;
  char* jobpath_loc;
  char* jobdatecommitted_loc;

  char* status_out;
  PyObject *status_out_py;

  std::cout << "\t Initialized " << std::endl;

  PyArg_ParseTupleAndKeywords(args, kwords,
                        "sslO|OOOO:set", kwlist,
                         &path_in,
                         &status_in,
                         &n_max,
                         &jobid,
                         &job,
                         &jobstage,
                         &jobpath,
                         &jobdatecommitted
                        );

  // Exit if we failed:
  if (!jobid) {
    status_out = "STATUS_FAILED_SPECIFIY_JOBID";
    return PyErr_Format(PyExc_AttributeError, status_out);
  } else if (!job && !jobstage && !jobpath && !jobdatecommitted) {
    status_out = "STATUS_FAILED_SPECIFY_ONE_OR_MORE_FIELDS";
    return PyErr_Format(PyExc_ValueError, status_out);
  } else if (PyLong_AsLong(jobid) > n_max) {
    status_out == "STATUS_INDEX_OUT_OF_BOUNDS";
    return PyErr_Format(PyExc_IndexError, status_out);
  }

  // Connect to persistent memory
  pool<pmem_queue> pop;
  if(init_pop(path_in, &pop)) {
    status_out = "STATUS_EMPTY"; 
  } else {
    status_out = "STATUS_OPEN_POTENTIALLY_FILLED";
  }

  std::cout << "\t Opened PM file " << std::endl;

  // reset q
  auto q = pop.get_root();

  // Set the entries of the index that are not null
  jobid_loc = PyLong_AsLong(jobid);
  if (job) { job_loc = PyBytes_AsString(job); } else { job_loc = NULL; }
  if (jobstage) { jobstage_loc = PyLong_AsLong(jobstage); } else { jobstage_loc = NULL; }
  if (jobpath) { jobpath_loc = PyBytes_AsString(jobpath); } else { jobpath_loc = NULL; }
  if (jobdatecommitted) {
    jobdatecommitted_loc = PyBytes_AsString(jobdatecommitted);
  } else {
    jobdatecommitted_loc = NULL;
  }

  if (!q->set(pop,
        jobid_loc,
        job_loc,
        jobstage_loc,
        jobpath_loc,
        jobdatecommitted_loc)) {
    pop.close();
    status_out = "STATUS_FAILED_SETTING_VALUES";
    return PyErr_Format(PyExc_AttributeError, status_out);
  } else {
    pop.close();
    status_out = "STATUS_SUCCCESS";
  }

  return Py_BuildValue("s", status_out);

}

PyObject* search(PyObject *self, PyObject *args, PyObject *kwords)
{
  /* Search database for objects fulfilling criteria,
   * returns index list specifying which indices fulfil these
   * which then can be used to get/set values.
   *
   * Use `only_first=True` to abort and return the first found element.
  */ 

  std::cout << "pmdb::search" << std::endl;

  // Input
  char* path_in, status_in;
  
  // Potential search objects, read in as keywords
  PyObject *jobid = nullptr;
  PyObject *job = nullptr;
  PyObject *jobstage = nullptr;
  PyObject *jobpath = nullptr;
  PyObject *jobdatecommitted = nullptr;

  static char *kwlist[] = {"path_in", "n_max",
    "jobid", "job", "jobstage", "jobpath", "jobdatecommitted", "only_first", NULL};

  // Will be parsed into these:
  int64_t n_max = NULL;
  int64_t jobid_loc;
  char* job_loc;
  int64_t jobstage_loc;
  char* jobpath_loc;
  char* jobdatecommitted_loc;
  bool only_first = false;  // returns only first element?

  char* status_out;
  PyObject *status_out_py;
  int64_t* query_is_true;
  PyObject *result_list;
  int64_t result_list_size, result_list_ix;

  std::cout << "\t Initialized " << std::endl;

  PyArg_ParseTupleAndKeywords(args, kwords,
                        "sl|OOOOOp:search", kwlist,
                         &path_in,
                         &n_max,
                         &jobid,
                         &job,
                         &jobstage,
                         &jobpath,
                         &jobdatecommitted,
                         &only_first
                        );

  // Exit if we failed:
  if (!path_in) {
    status_out = "STATUS_FAILED_SPECIFY_PATH_IN";
    return PyErr_Format(PyExc_ValueError, status_out);
  } else if (!n_max) {
    status_out = "STATUS_FAILED_SPECIFY_N_MAX";
    return PyErr_Format(PyExc_ValueError, status_out);
  } else if (!jobid && !job && !jobstage && !jobpath && !jobdatecommitted) {
    status_out = "STATUS_FAILED_SPECIFY_ONE_OR_MORE_FIELDS";
    return PyErr_Format(PyExc_ValueError, status_out);
  } 

  // Each query is a tuple of an operator and a statement,
  // ie jobid=('>', 3)
  // will return all jobids greater than 3.

	int64_t q_status;

  char* oper_jobid = nullptr;
  char* oper_job = nullptr;
  char* oper_jobstage = nullptr;
  char* oper_jobpath = nullptr;
  char* oper_datecommitted = nullptr;

  
  int64_t jobid_q = -1;
  char* job_q = nullptr;
  int64_t jobstage_q = -1;
  char* jobpath_q = nullptr;
  char* datecommitted_q = nullptr;

  if (jobid) {
    oper_jobid = new char[8];
    PyArg_ParseTuple(jobid, "sl", &oper_jobid, &jobid_q);
  }
  if (job) {
    oper_job = new char[8];
    PyArg_ParseTuple(job, "ss", &oper_job,  &job_q);
  }
  if (jobstage) {
    oper_jobstage = new char[8];
    PyArg_ParseTuple(jobstage, "sl", &oper_jobstage, &jobstage_q);
  }
  if (jobpath) {
    oper_jobpath = new char[8];
    PyArg_ParseTuple(jobpath, "ss", &oper_jobpath, &jobpath_q);
  }
  if (jobdatecommitted) {
    oper_datecommitted = new char[8];
    PyArg_ParseTuple(jobdatecommitted, "ss", &oper_datecommitted, &datecommitted_q);
  }

  std::cout << "Found operator of jobid: " << oper_jobid << std::endl;

  // Connect to persistent memory
  pool<pmem_queue> pop;
  if(init_pop(path_in, &pop)) {
    status_out = "STATUS_EMPTY"; 
  } else {
    status_out = "STATUS_OPEN_POTENTIALLY_FILLED";
  }

  std::cout << "\t Opened PM file " << std::endl;

  // reset q
  auto q = pop.get_root();

  // Go through all the elements and return the indices where they are true

  // Will interface with the search function, each index has an entry
  query_is_true = new int64_t[n_max];

  // The search function loops over the data and sets the values of query_is_true
	q_status = q->search_all(
      query_is_true, n_max,
      oper_jobid, jobid_q,
      oper_job, job_q,
      oper_jobstage, jobstage_q,
      oper_jobpath, jobpath_q,
      oper_datecommitted, datecommitted_q,
      only_first);

  pop.close();

  // Generate result list for Python, returning only indices
  // First, find size of it:
  result_list_size = 0;
  if (!only_first) {
    for (int ii=0; ii < n_max; ii++) {
      if (query_is_true[ii] != 0) {
        result_list_size += 1;
      }
    }
  } else {
    result_list_size = 1;
  }
  
  // Make it
  result_list = PyList_New(result_list_size);

  // Insert indices:
  result_list_ix = 0;
  if (!only_first) {
    for (int ii=0; ii < n_max; ii++) {
      if (query_is_true[ii] != 0) {
        PyList_SetItem(result_list, result_list_ix, PyLong_FromLong(ii));
        result_list_ix += 1;
      }
    }
  } else {
    // Sets the first element to the first index of occurrence
    // given from the q_status output
    if (q_status < n_max) {
      PyList_SetItem(result_list, 0, PyLong_FromLong(q_status));
    }
  }

  delete query_is_true;

  return result_list;

}


static PyMethodDef pmdb_methods[] = {
    { "init_pmdb", (PyCFunction)init_pmdb, METH_VARARGS, nullptr },
    { "insert", (PyCFunction)insert, METH_VARARGS, nullptr },
    { "get", (PyCFunction)get, METH_VARARGS, nullptr },
    { "set", (PyCFunction)set, METH_VARARGS|METH_KEYWORDS, nullptr },
    { "search", (PyCFunction)search, METH_VARARGS|METH_KEYWORDS, nullptr },

    // Terminate the array with an object containing nulls.
    { nullptr, nullptr, 0, nullptr }
};

static PyModuleDef pmdb_module = {
    PyModuleDef_HEAD_INIT,
    "pmdb",                        // Module name to use with Python import statements
    "Permament Memory Database Python bindings",  // Module description
    0,
    pmdb_methods // Structure that defines the methods of the module
};

PyMODINIT_FUNC PyInit_pmdb()
{
    return PyModule_Create(&pmdb_module);
}

/////// END PYTHON EXTENSION ////////

