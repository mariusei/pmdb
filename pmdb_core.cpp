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
 *
 */


#include "pmdb_core.hpp"
#include "pmdb_helpers.hpp"

using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::pool_base;
using pmem::obj::make_persistent;
using pmem::obj::delete_persistent;
using pmem::obj::transaction;





/********************************************
 *
 * PMEM_QUEUE::PUSH
 * pusher
 *
 *******************************************/
void pmem_queue::push(
    pool_base &pop,
    int64_t jobid = NULL,
    char job[MAX_JOB_SIZE] = NULL,
    int64_t jobstage = NULL,
    char savepath[MAX_JOB_SIZE] = NULL,
    char datecommitted[MAX_JOB_SIZE] = NULL,
    int64_t jobtagged = NULL
    )
{
  uint64_t j;
  char empty[255] = "\0";
  transaction::exec_tx(pop, [&] {
      auto n = make_persistent<pmem_queue::pmem_entry>();

      n->jobid = jobid;
      
      if (job) { pstrcpy(n->job, job); } else { pstrcpy(n->job, empty); }
      if (jobstage) { n->jobstage = jobstage; } else { n->jobstage = 0; }
      if (savepath) { pstrcpy(n->savepath, savepath); } else { pstrcpy(n->savepath, empty); }
      if (datecommitted) { pstrcpy(n->datecommitted, datecommitted); } else { pstrcpy(n->datecommitted, empty); }
      if (jobtagged) { n->jobtagged = jobtagged; } else { n->jobtagged = 0; }
      n->next = NULL;

      //std::cout << "\t\t pushed: " << n->jobid << " with job " << n->job << " and " << n->jobstage << " " << n->savepath << " " << n->datecommitted << std::endl;

      if (head == NULL) {
        head = tail = n;
      } else {
        tail->next = n;
        tail = n;
      }

   });

}

/********************************************
 *
 * PMEM_QUEUE::GET
 * getter
 *
 *******************************************/
entry_shared pmem_queue::get(uint64_t ix)
{
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
      out.jobtagged = n->jobtagged;
      
      //std::cout << "GOT in get: "<< out.jobid << " and jobstage " << n->jobstage << " and job " << out.job << std::endl;

      return out;
    }
    i+= 1;
    } 
}

/********************************************
 *
 * PMEM_QUEUE::SET
 * setter
 *
 *******************************************/
bool pmem_queue::set(
    pool_base &pop,
    int64_t jobid,
    char* job = NULL,
    int64_t jobstage = -1,
    char* savepath = NULL,
    char* datecommitted = NULL,
    int64_t jobtagged = -1 
    )
{
  uint64_t i = 0;
  uint64_t j = 0;
  auto n = head;

  std::cerr << "IN SET FOR JOBID " << jobid << std::endl;

  // Spool forward to the correct item
  for (n; n != NULL; n = n->next) {
    //std::cerr << i << std::endl;
    if (i == jobid) { 
      //std::cerr << "will replace at " << n->jobid << " with jobstage " << jobstage << std::endl;
      transaction::exec_tx(pop, [&] {
          if (job) {
          //std::cerr << "JOB" << std::endl;
            pstrcpy(n->job, job);
          }
          if (jobstage != -1) {
            //std::cerr << "JOBSTAGE" << std::endl;
            n->jobstage = jobstage; 
          }
          if (savepath) {
            //std::cerr << "SAVEPATH" << std::endl;
            pstrcpy(n->savepath, savepath);
          }
          if (datecommitted) {
            //std::cerr << "DATECOMITTED" << std::endl;
            pstrcpy(n->datecommitted, datecommitted);
          }
          if (jobtagged != -1) {
            //std::cerr << "JOBTAGGED" << std::endl;
            n->jobtagged = jobtagged; 
          }
      });
      return true;
    }
    i+= 1;
  } 
  return false;
}

int64_t pmem_queue::count(void)
{
  int64_t out_n = 0;

  for (auto n = head; n != nullptr; n = n->next) {
    out_n += 1;
  }
  return out_n;
}


/********************************************
 *
 * PMEM_QUEUE::SHOW
 * showmaker
 *
 *******************************************/
void pmem_queue::show(void) const
{
  for (auto n = head; n != nullptr; n = n->next) {
    std::cout << n->jobid << " with job " << n->job << " committed at " << n->datecommitted << std::endl;
  }
  return;
}

/********************************************
 *
 * PMEM_QUEUE::SEARCH_ALL
 * searcher
 *
 *******************************************/
int64_t pmem_queue::search_all(
    int64_t* out_array,
    int64_t n_el,
    const char* jobid_oper=nullptr,
    int64_t jobid_q=-1,
    const char* job_oper=nullptr,
    const char* job_q=nullptr,
    const char* jobstage_oper=nullptr,
    int64_t jobstage_q=-1,
    const char* jobpath_oper=nullptr,
    const char* jobpath_q=nullptr,
    const char* jobdatecommitted_oper=nullptr,
    const char* jobdatecommitted_q=nullptr,
    const char* jobtagged_oper=nullptr,
    int64_t jobtagged_q=-1,
    bool only_first=false)
{
  // Will search n_el starting from 0 for the conditions and queries given
  // and assign out_array[i] = True where the queries match
  //
  // If only_first is true, will abort at first chance and return the index

  std::cout << "IN SEARCH" << std::endl;
  auto n = head;
  int64_t i = 0;
  int64_t res;

  //std::cout << "job tagged q: " << n->jobtagged << jobtagged_oper << jobtagged_q << std::endl;


  // Spool forward to the correct item
  for (n; n != NULL; n = n->next) {
    //std::cout << i << std::endl;
    res = 1; // assume true 
    if (jobid_oper) {
      res *= query(n->jobid, jobid_oper, jobid_q);
    }
    if (job_oper) {
      res *= query_str(n->job, job_oper, job_q);
    }
    if (jobstage_oper) {
      res *= query(n->jobstage, jobstage_oper, jobstage_q);
    }
    if (jobpath_oper) {
      res *= query_str(n->savepath, jobpath_oper, jobpath_q);
    }
    if (jobdatecommitted_oper) {
      res *= query_str(n->datecommitted, jobdatecommitted_oper, jobdatecommitted_q);
    }
    if (jobtagged_oper) {
      res *= query(n->jobtagged, jobtagged_oper, jobtagged_q);
    }
    
    // Check if we shall abort?
    if (only_first) {
      if (res) {
        return i;
      }
    }


    out_array[i] = res;
    i += 1;
  }

  return i;
}



