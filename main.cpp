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

int main() {
  printf("hei!\n");

  std::string path("test.pmem");
  const char *cpath = path.c_str();

  std::cout << "hei igjen, lagrer til " << path << "\n";

  pool<pmem_queue> pop;
  
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
