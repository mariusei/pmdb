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

#ifndef PMDB_CORE_HPP
#define PMDB_CORE_HPP

#include "ex_common.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <Python.h>

#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>


#define MAX_JOB_SIZE 256

/* 
 * Use entry_shared to get data
 * from persistent memory
 * in pmem_queue::get
 *
 */
typedef struct {
  int64_t jobid;
  char job[MAX_JOB_SIZE];
  int64_t jobstage;
  char savepath[MAX_JOB_SIZE];
  char datecommitted[MAX_JOB_SIZE];
  int64_t jobtagged;
} entry_shared;


/*
 * pmem_queue:
 *
 * Functional list implementation with
 * added fields to make it usable as a database
 * for github.com/mariusei/job_manager
 *
 *
 * Based upon:
 *
 * github.com/pmem/pmdk/blob/master/src/examples/libpmemobj%2B%2B/queue/queue.cpp
 * by Intel Corporation
 *
 * and https://www.usenix.org/system/files/login/articles/login_summer17_07_rudoff.pdf
 * by Andy Rudoff (2017).
 *
 */

class pmem_queue {

  struct pmem_entry {
    pmem::obj::persistent_ptr<pmem_entry> next;
    pmem::obj::p<int64_t> jobid;
    pmem::obj::p<char> job[MAX_JOB_SIZE];
    pmem::obj::p<int64_t> jobstage;
    pmem::obj::p<char> savepath[MAX_JOB_SIZE];
    pmem::obj::p<char> datecommitted[MAX_JOB_SIZE];
    pmem::obj::p<int64_t> jobtagged;
  };


public:

  void push(
      pmem::obj::pool_base &pop,
      int64_t jobid,
      char job[MAX_JOB_SIZE],
      int64_t jobstage,
      char savepath[MAX_JOB_SIZE],
      char datecommitted[MAX_JOB_SIZE],
      int64_t jobtagged
      );

  entry_shared get(uint64_t ix);

  bool set(
      pmem::obj::pool_base &pop,
      int64_t jobid,
      char* job,
      int64_t jobstage,
      char* savepath,
      char* datecommitted,
      int64_t jobtagged
      );

  int64_t count(void);
  void show(void) const;

  int64_t search_all(
      int64_t* out_array,
      int64_t n_el,
      const char* jobid_oper, 
      int64_t jobid_q,
      const char* job_oper, 
      const char* job_q,
      const char* jobstage_oper,
      int64_t jobstage_q,
      const char* jobpath_oper,
      const char* jobpath_q,
      const char* jobdatecommitted_oper,
      const char* jobdatecommitted_q,
      const char* jobtagged_oper,
      int64_t jobtagged_q,
      bool only_first);


private:
  pmem::obj::persistent_ptr<pmem_entry> head;
  pmem::obj::persistent_ptr<pmem_entry> tail;
};

#endif
