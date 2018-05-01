
#include "ex_common.h"

#include <stdint.h>
#include <stdio.h>
#include <errno.h>
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

typedef struct {
  int64_t jobid;
  char* job;
  int64_t jobstage;
  char* savepath;
  char* datecommitted;
} entry_shared;







class pmem_queue {


  struct pmem_entry {
    persistent_ptr<pmem_entry> next;
    p<int64_t> jobid;
    p<char*> job;
    p<int64_t> jobstage;
    p<char*> savepath;
    p<char*> datecommitted;
  };


public:

  void push(pool_base &pop,
      int64_t jobid,
      char job[256],
      int64_t jobstage,
      char savepath[256],
      char datecommitted[256]
      ) {
    transaction::exec_tx(pop, [&] {
        auto n = make_persistent<pmem_entry>();

        n->jobid = jobid;
        n->job = job;
        n->jobstage = jobstage;
        n->savepath = savepath;
        n->datecommitted = datecommitted;
        n->next = NULL;

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
    auto n = head;
    entry_shared out;

    for (n; n != nullptr ; n = n->next) {
      //std::cout << i << std::endl;
      if (i == ix) { 
        // returns:
        out.jobid = n->jobid;
        out.job = n->job;
        out.jobstage = n->jobstage;
        out.savepath = n->savepath;
        out.datecommitted = n->datecommitted;
        return out;
      }
      i+= 1;
      } 
    }

  bool set(pool_base &pop,
      int64_t jobid,
      char* job=NULL,
      int64_t jobstage=NULL,
      char* savepath=NULL,
      char* datecommitted=NULL
      ) {

    uint64_t i = 0;
    auto n = head;
    for (n; n != NULL; n = n->next) {
      std::cout << i << std::endl;
      if (i == jobid) { 
        std::cout << "will replace at " << n->jobid << " with jobstage " << jobstage << std::endl;
        transaction::exec_tx(pop, [&] {
            if (job) { n->job = job; }
            if (jobstage) { n->jobstage = jobstage; }
            if (savepath) { n->savepath = savepath; }
            if (datecommitted) { n->datecommitted = datecommitted ; }
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
        path, "queue", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
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
  
  return path_back_py;

error:
  return Py_BuildValue("");

}

PyObject* insert(PyObject *self, PyObject *args) {
  /* Inserts a single element into the database.
     Must be called sequentially until all n objects
     have been inserted.
  */ 

  // Input
  char* path_in, status_in;
  int64_t n_max;
  // Will be inserted in DB:
  int64_t jobid;
  char* job;
  int64_t jobstage;
  char* jobpath;
  char* jobdatecommitted;

  char* status_out;
  PyObject *status_out_py;

  pool<pmem_queue> pop;
  if(init_pop(path_in, &pop)) {
    status_out = "STATUS_EMPTY"; 
  } else {
    status_out = "STATUS_OPEN_POTENTIALLY_FILLED";
  }

  if (status_out == "STATUS_EMPTY") {
    // The database should have been initialized
    status_out = "STATUS_FAILED_NOT_INITIALIZED";
    status_out_py = Py_BuildValue("s", status_out);
    return status_out_py;
  }


  auto q = pop.get_root();

  if (!PyArg_ParseTuple(args, "ssllSlss", 
                         &path_in,
                         &status_in,
                         &n_max,
                         &jobid,
                         &job,
                         &jobstage,
                         &jobpath,
                         &jobdatecommitted
                         )) goto error;

  // All is set, push to list
  q->push(pop, 
          jobid,
          job,
          jobstage,
          jobpath,
          jobdatecommitted
      );

  status_out = "STATUS_INSERTED";
  status_out_py = Py_BuildValue("s", status_out);
  return status_out_py;
  
error:
  return Py_BuildValue("");
}



static PyMethodDef pmdb_methods[] = {
    // The first property is the name exposed to Python, fast_tanh, the second is the C++
    // function name that contains the implementation.
    { "init_pmdb", (PyCFunction)init_pmdb, METH_VARARGS, nullptr },
    { "insert", (PyCFunction)insert, METH_VARARGS, nullptr },

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
