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

#ifndef PMDB_PYTHON_INTERFACE_HPP
#define PMDB_PYTHON_INTERFACE_HPP

#include "pmdb_core.hpp"
#include <Python.h>

///////   PYTHON EXTENSION   ////////

bool init_pop(char* path, pmem::obj::pool<pmem_queue> *pop);

PyObject* init_pmdb(PyObject *self, PyObject* args, PyObject *kwords);

PyObject* insert(PyObject *self, PyObject *args, PyObject *kwords);

/* Gets a single object,
 * returns it as a Python list 
*/ 
PyObject* get(PyObject *self, PyObject *args);

/* Raises exception with error string `errstring`,
 * of type `exception` (e.g. PyValueError) and
 * optionally returns a Python object `element`.
 */
PyObject* set_pyerr(const char* errstring, PyObject* element=NULL);

/* Sets values to a single entry in the database
 * returns status code
*/ 
PyObject* set(PyObject *self, PyObject *args, PyObject *kwords);

/* Search database for objects fulfilling criteria,
 * returns index list specifying which indices fulfil these
 * which then can be used to get/set values.
 *
 * Use `only_first=True` to abort and return the first found element.
*/ 
PyObject* search(PyObject *self, PyObject *args, PyObject *kwords);

PyObject* count(PyObject *self, PyObject *args, PyObject *kwords);

PyMODINIT_FUNC PyInit_pmdb();

#endif
