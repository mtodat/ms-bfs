//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <cstdint>
#include <cassert>
#include <string>

struct Tokenizer {
   const uint8_t* end;

   uint8_t* iter;

   Tokenizer(void* data, uint64_t size)
      : end(reinterpret_cast<uint8_t*>(data)+size), iter(reinterpret_cast<uint8_t*>(data)) {
   }

   inline uint64_t readId(const char delim) {
      uint64_t id=0;
      while(iter!=end && *iter != delim) {
         assert(*iter>='0' && *iter<='9');
         id=id*10+(*iter-'0');
         iter++;
      }

      if(*iter==delim) {
         iter++;
      }

      return id;
   }

   inline std::string readStr(const char delim) {
      const unsigned char* start = iter;
      size_t length = 0;
      while(iter!=end && *iter != delim) {
         length++;
         iter++;
      }

      std::string result((char*)start, length);

      if(*iter==delim) {
         iter++;
      }

      return result;
   }

   inline void skipLine() {
      while(iter!=end && *iter != '\n') {
         iter++;
      }

      if(*iter == '\n') {
         iter++;
      }
   }

   inline bool isFinished() {
      return iter==end;
   }
};