//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "sse.hpp"
#include <bitset>
#include <cmath>

template<typename bit_t>
struct CtzlOp {
   static size_t ctzl(bit_t value) {
      // Only works for uint64_t
      static_assert(sizeof(bit_t)==8, "Only works for 64bit values");
      return __builtin_ctzl(value);
   }
};

template<>
struct CtzlOp<uint32_t> {
   static size_t ctzl(uint32_t value) {
      return __builtin_ctz(value);
   }
};
template<>
struct CtzlOp<uint16_t> {
   static size_t ctzl(uint16_t value) {
      return __builtin_ctz(value);
   }
};

template<>
struct CtzlOp<uint8_t> {
   static size_t ctzl(uint8_t value) {
      return __builtin_ctz(value);
   }
};

template<typename bit_t>
struct BitBaseOp {
private:
   static const size_t TYPE_BITS_COUNT = sizeof(bit_t)*8;
   bit_t set[TYPE_BITS_COUNT];

   BitBaseOp() {
      for (unsigned i = 0; i < sizeof(bit_t)*8; ++i) {
         set[i] = pow(((bit_t)2),i);
      }
   }
   static const BitBaseOp masks;

public:
   static bit_t getSetMask(const size_t bitPos) {
      //return ((bit_t)1) << pos;
      return masks.set[bitPos];
   }

   static bit_t andNot(const bit_t a, const bit_t b) {
      return a & ~b;
   }

   static bool isZero(const bit_t value) {
      return value == 0;
   }

   static bool notZero(const bit_t value) {
      return value != 0;
   }

   static bool notAllOnes(const bit_t value) {
      return value != std::numeric_limits<bit_t>::max();
   }

   //Checks if bitwise and of two values is nonzero
   static bool andNotZero(const bit_t a, const bit_t b) {
      return (a&b)!=0;
   }

   static unsigned popCount(const bit_t a) {
      return std::bitset<TYPE_BITS_COUNT>(a).count();
   }

   static inline bit_t zero() {
      return 0;
   }

   static bool equals(const bit_t a, const bit_t b) {
      return a == b;
   }

   static bool notEquals(const bit_t a, const bit_t b) {
      return a != b;
   }
};

// Normal logic again
template<typename bit_t> const BitBaseOp<bit_t> BitBaseOp<bit_t>::masks = BitBaseOp<bit_t>();

// SSE specialization
template<>
struct CtzlOp<__m128i> {
   static size_t ctzl(__m128i value) {
      const uint64_t a = _mm_extract_epi64(value, 0);
      const uint64_t b = _mm_extract_epi64(value, 1);
      //assert(a!=0||b!=0);
      return a==0?__builtin_ctzl(b)+64:__builtin_ctzl(a);
   }
};

template<>
struct BitBaseOp<__m128i> {
   static __m128i getSetMask(const size_t bitPos) {
      return sseMasks[bitPos];
   }

   static __m128i andNot(const __m128i a, const __m128i b) {
      return _mm_andnot_si128(b, a);
   }

   //Checks if bitwise and of two values is nonzero
   static bool andNotZero(const __m128i a, const __m128i b) {
      return _mm_testz_si128(a,b)==0;
   }

   static bool isZero(const __m128i value) {
      return _mm_testc_si128(_mm_setzero_si128(),value)!=0;
   }

   static bool notAllOnes(const __m128i value) {
      return _mm_test_all_ones(value)==0;
   }

   static bool notZero(const __m128i value) {
      return _mm_testc_si128(_mm_setzero_si128(),value)==0;
   }

   static unsigned popCount(const __m128i value) {
      return __builtin_popcountl(_mm_extract_epi64(value,0)) + __builtin_popcountl(_mm_extract_epi64(value,1));
   }

   static inline __m128i zero() {
      return _mm_setzero_si128();
   }

   static bool equals(const __m128i a, const __m128i b) {
      const __m128i cmp = _mm_cmpeq_epi64(a, b);
      return _mm_extract_epi64(cmp,0)!=0 && _mm_extract_epi64(cmp,1)!=0;
   }

   static bool notEquals(const __m128i a, const __m128i b) {
      const __m128i cmp = _mm_cmpeq_epi64(a, b);
      return _mm_extract_epi64(cmp,0)==0 || _mm_extract_epi64(cmp,1)==0;
   }
};

#ifdef AVX2
template<>
struct CtzlOp<__m256i> {
   static size_t ctzl(__m256i value) {
      const uint64_t a = _mm256_extract_epi64(value, 0);
      const uint64_t b = _mm256_extract_epi64(value, 1);
      const uint64_t c = _mm256_extract_epi64(value, 2);
      const uint64_t d = _mm256_extract_epi64(value, 3);
      if(a==0) {
         if(b==0) {
            return c==0?__builtin_ctzl(d)+192:__builtin_ctzl(c)+128;
         } else {
            return __builtin_ctzl(b)+64;
         }
      }
      return __builtin_ctzl(a);
   }
};

template<>
struct BitBaseOp<__m256i> {
   static __m256i getSetMask(const size_t bitPos) {
      return sseMasks256[bitPos];
   }

   static __m256i andNot(const __m256i a, const __m256i b) {
      return _mm256_andnot_si256(b, a);
   }

   static bool isZero(const __m256i value) {
      return _mm256_testc_si256(_mm256_setzero_si256(), value)!=0;
   }

   static bool notZero(const __m256i value) {
      return _mm256_testc_si256(_mm256_setzero_si256(), value)==0;
   }

   static bool notAllOnes(const __m256i value) {
      static const __m256i ones=_mm256_set_epi64x(std::numeric_limits<uint64_t>::max(),std::numeric_limits<uint64_t>::max(),std::numeric_limits<uint64_t>::max(),std::numeric_limits<uint64_t>::max());
      return _mm256_testc_si256(value, ones)==0;
   }

   static unsigned popCount(const __m256i value) {
      return __builtin_popcountl(_mm256_extract_epi64(value,0))
         + __builtin_popcountl(_mm256_extract_epi64(value,1))
         + __builtin_popcountl(_mm256_extract_epi64(value,2))
         + __builtin_popcountl(_mm256_extract_epi64(value,3));
   }

   static inline __m256i zero() {
      return _mm256_setzero_si256();
   }
};
#endif