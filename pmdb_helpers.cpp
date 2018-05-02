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

#include <stdint.h>
#include <cstring>

#include <libpmemobj++/p.hpp>


template <typename T, typename U>
int strcmp2(T str1, U str2)
{
// https://stackoverflow.com/questions/20004458/optimized-strcmp-implementation
// Sergey Belyashov
    int s1;
    int s2;
    do {
        s1 = *str1++;
        s2 = *str2++;
        if (s1 == 0)
            break;
    } while (s1 == s2);
    return (s1 < s2) ? -1 : (s1 > s2);
}

//template <typename T, typename U>
//int64_t query(T a, const char* q, U b)
int64_t query(pmem::obj::p<int64_t> a, char const* q, int64_t b)
{
//inline int64_t query(int64_t a, const char* q, int64_t b) {
  // parse q into: operator and value
  
  //std::cout << "in query w/ " << a << q << b << std::endl;
  
  const char* o[] = {"==", ">=", "<=", ">", "<", "!="};

  // ==
  if (strcmp(q, o[0]) == 0) {
    return (a == b);
  // >=
  } else if (strcmp(q, o[1])==0) {
    return (a >= b);
  } else if (strcmp(q, o[2])==0) {
    return (a <= b);
  } else if (strcmp(q, o[3])==0) {
    return (a > b);
  } else if (strcmp(q, o[4])==0) {
    return (a < b);
  } else if (strcmp(q, o[5])==0) {
    return (a != b);
  } else {
    return -1;
  }
}

//template <typename T, typename U>
//int64_t query_str(T a, const char* q, U b)
int64_t query_str(pmem::obj::p<char>* a, char const* q, char const* b)
{
//inline int64_t query_str(const char* a, const char* q, const char* b) {
  // Parse q into: operator and value
  
  const char* o[] = {"==", "!="};

  // ==
  if (strcmp(q, o[0]) == 0) {
    if (strcmp2(a,b)==0) { return 1; }
    else { return 0; }
  } else if (strcmp(q, o[1]) == 0) {
    if (strcmp2(a,b)!=0) { return 1; }
    else { return 0; }
  } else {
    return -1;
  }
}
