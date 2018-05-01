
#include "ex_common.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
//#include <errno.h>
#include <stdlib.h>

#include <Python.h>

#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::pool_base;
using pmem::obj::make_persistent;
using pmem::obj::delete_persistent;
using pmem::obj::transaction;

#define MAX_JOB_SIZE 256

typedef struct {
  int64_t jobid;
  char job[MAX_JOB_SIZE];
  int64_t jobstage;
  char savepath[MAX_JOB_SIZE];
  char datecommitted[MAX_JOB_SIZE];
} entry_shared;

template <typename T, typename U>
inline bool pstrcpy(T strout, U strin) {
  uint64_t j;
  for (j=0; strin[j] != '\0'; j++) {
  //for (j=0; j<MAX_JOB_SIZE; j++) {
    strout[j] = strin[j];
  }
  strout[j] = '\0';
  return true;
}

class pmem_queue {


  struct pmem_entry {
    persistent_ptr<pmem_entry> next;
    p<int64_t> jobid;
    p<char> job[MAX_JOB_SIZE];
    p<int64_t> jobstage;
    p<char> savepath[MAX_JOB_SIZE];
    p<char> datecommitted[MAX_JOB_SIZE];
  };


public:

  void push(pool_base &pop,
      int64_t jobid,
      char job[MAX_JOB_SIZE],
      int64_t jobstage,
      char savepath[MAX_JOB_SIZE],
      char datecommitted[MAX_JOB_SIZE]
      ) {
    uint64_t j;
    transaction::exec_tx(pop, [&] {
        auto n = make_persistent<pmem_entry>();

        n->jobid = jobid;
        pstrcpy(n->job, job);
        n->jobstage = jobstage;
        pstrcpy(n->savepath, savepath);
        pstrcpy(n->datecommitted, datecommitted);
        n->next = NULL;

        std::cout << "\t\t pushed: " << n->jobid << " with job " << n->job << " and " << n->jobstage << " " << n->savepath << " " << n->datecommitted << std::endl;

        if (head == NULL) {
          head = tail = n;
        } else {
          tail->next = n;
          tail = n;
        }

     });

  }

  entry_shared get(uint64_t ix) {

    uint64_t i = 0;
    uint64_t j;
    auto n = head;
    entry_shared out;

    for (n; n != nullptr ; n = n->next) {
      //std::cout << i << std::endl;
      if (i == ix) { 
        // returns:
        out.jobid = n->jobid;
        pstrcpy(out.job, n->job);
        out.jobstage = n->jobstage;
        pstrcpy(out.savepath, n->savepath);
        pstrcpy(out.datecommitted, n->datecommitted);
        
        std::cout << "GOT in get: "<< out.jobid << " and jobstage " << n->jobstage << " and job " << out.job << std::endl;

        return out;
      }
      i+= 1;
      } 
    }

  bool set(pool_base &pop,
      int64_t jobid,
      char job[MAX_JOB_SIZE]=NULL,
      int64_t jobstage=NULL,
      char savepath[MAX_JOB_SIZE]=NULL,
      char datecommitted[MAX_JOB_SIZE]=NULL
      ) {

    uint64_t i = 0;
    uint64_t j = 0;
    auto n = head;

    for (n; n != NULL; n = n->next) {
      std::cout << i << std::endl;
      if (i == jobid) { 
        std::cout << "will replace at " << n->jobid << " with jobstage " << jobstage << std::endl;
        transaction::exec_tx(pop, [&] {
            if (job) {
              pstrcpy(n->job, job);
            }
            if (jobstage) { n->jobstage = jobstage; }
            if (savepath) {
              pstrcpy(n->savepath, savepath);
            }
            if (datecommitted) {
              pstrcpy(n->datecommitted, datecommitted);
            }
        });
        return true;
      }
      i+= 1;
      } 
    return false;
    }

    int64_t count(void) const {
      int64_t out_n = 0;

      for (auto n = head; n != nullptr; n = n->next) {
        out_n += 1;
      }
      return out_n;
    }

    persistent_ptr<pmem_entry> get_last(void) {
      persistent_ptr<pmem_entry> n;
      for (n = head; n != nullptr; n = n->next)
      {
      }
      return n;
    }


    void show(void) const {

    for (auto n = head; n != nullptr; n = n->next) {
      std::cout << n->jobid << " with job " << n->job << " committed at " << n->datecommitted << std::endl;
    }
    return;
    }

private:
  persistent_ptr<pmem_entry> head;
  persistent_ptr<pmem_entry> tail;

};

class metadata {
public:
  p<int64_t> n_el;
  p<int64_t> data_is_set;

  metadata();
  metadata(int64_t);
  bool set_nel(int64_t);
  bool get_nel(int64_t* out);
  bool set_locked();
};

metadata::metadata() : n_el(0)
{
};

metadata::metadata(int64_t n_el) : n_el(n_el)
{
};

bool metadata::set_nel(int64_t nel)
{
  n_el = nel;
  return true;
}

bool metadata::get_nel(int64_t* out)
{
  out[0] = n_el;
  return true;
}

bool metadata::set_locked()
{
  data_is_set = 1;
  return true;
}


///////   PYTHON EXTENSION   ////////

bool init_pop(char* path, pool<pmem_queue> *pop) {
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

PyObject* init_pmdb(PyObject *self, PyObject* args) {
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

PyObject* insert(PyObject *self, PyObject *args) {
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

PyObject* get(PyObject *self, PyObject *args) {
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




static PyMethodDef pmdb_methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "init_pmdb", (PyCFunction)init_pmdb, METH_VARARGS, nullptr },
    { "insert", (PyCFunction)insert, METH_VARARGS, nullptr },
    { "get", (PyCFunction)get, METH_VARARGS, nullptr },

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

PyMODINIT_FUNC PyInit_pmdb() {
    return PyModule_Create(&pmdb_module);
}


/////// END PYTHON EXTENSION ////////

int main() {
  printf("hei!\n");

  std::string path("test.pmem");
  //std::string metadatapath("info.pmem");
  const char *cpath = path.c_str();
  //const char *ipath = metadatapath.c_str();

  std::cout << "hei igjen, lagrer til " << path << "\n";

  pool<pmem_queue> pop;
  //pool<metadata> pinfo;
  
  int64_t n_el = 123;
  bool data_is_set = false;


  // Database storage
  if (file_exists(cpath) != 0) {
    pop = pool<pmem_queue>::create(
        cpath, "queue", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
  } else {
      pop = pool<pmem_queue>::open(cpath, "queue");
      data_is_set = true;
  }

  //// Metadata storage
  //if (file_exists(ipath) != 0) {
  //  pinfo = pool<metadata>::create(
  //      ipath, "metadata", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
  //} else {
  //    pinfo = pool<metadata>::open(ipath, "metadata");
  //}

  //metadata info(123);
  auto q = pop.get_root();

  if (!data_is_set) {
    for (int i=0; i< n_el ; i++) {
      q->push(pop,
          i,
          "\x80\x03]q\x00(M\x00(JM$\x03\x00e.",
          1,
          "out/test1.py",
          "2018-03-29 12:01:02"
              );
    }
  }

  //q->show();

  entry_shared res;
  for (int i=0; i< n_el ; i++) {
    res = q->get(i);
    std::cout << "Got res: " << res.jobid << " jobstage: " << res.jobstage << " datecommitted: " << res.datecommitted << std::endl;
  }

  //std::cout << "Got res:" << res.jobid << " " << res.job << " " << res.jobstage << " " << res.datecommitted << std::endl;


  pop.close();

  return 0;


}
