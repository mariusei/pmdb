#ifndef PMDB_HELPERS_HPP
#define PMDB_HELPERS_HPP

#include <libpmemobj++/p.hpp>


template <typename T, typename U>
inline bool pstrcpy(T strout, U strin)
{
  uint64_t j;
  for (j=0; strin[j] != '\0'; j++) {
    strout[j] = strin[j];
  }
  strout[j] = '\0';
  return true;
}

template <typename T, typename U>
int strcmp2(T str1, U str2);
// https://stackoverflow.com/questions/20004458/optimized-strcmp-implementation
// Sergey Belyashov

//template <typename T, typename U>
//int64_t query(T a, const char* q, U b);
//
//template <typename T, typename U>
//int64_t query_str(T a, const char* q, U b);

int64_t query(pmem::obj::p<long>, char const*, long);
int64_t query_str(pmem::obj::p<char>*, char const*, char const*);


#endif
