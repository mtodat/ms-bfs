//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "sse.hpp"
#include "../macros.hpp"

#include <cassert>
#include <array>
#include <cstring>

namespace Query4 {

template<typename bit_t, uint64_t width>
struct BatchDistance {
   static const bit_t allZeros = 0;
   static constexpr bit_t byteOnes = sizeof(bit_t)==8 ? 0x0101010101010101ul : 0x01010101;
   static const bit_t lastByte = 0xFF;

   uint32_t* numDistDiscovered;
   uint8_t localDiscoveredIteration[width];
   alignas(16) bit_t localDiscovered[8*width];

   BatchDistance(uint32_t* numDistDiscovered) : numDistDiscovered(numDistDiscovered) {
      memset(localDiscovered, 0, sizeof(bit_t)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }

   BatchDistance& operator=(BatchDistance&& other) = delete;

   void updateDiscoveredNodes(const uint64_t partIx) {
      for(unsigned bit=0; bit<8; bit++) {
         const uint64_t localOffset = partIx*8+bit;
         const uint64_t baseOffset = partIx*sizeof(bit_t)*8+bit;
         for(unsigned i=0; i<sizeof(bit_t); i++) {
            numDistDiscovered[baseOffset+8*i] += (localDiscovered[localOffset] >> 8*i) & lastByte;
         }
      }
      for(int bit=0; bit<8; bit++) {
         localDiscovered[partIx*8+bit] = allZeros;
      }
   }

   inline void prefetch(uint64_t partIx) {
      __builtin_prefetch(&localDiscovered[partIx*sizeof(bit_t)],1);
   }

   void updateDiscovered(bit_t newVisits, uint64_t partIx) {
      assert(partIx<=width);
      uint64_t baseOffset = partIx*8;

      localDiscovered[baseOffset+0] += newVisits & byteOnes;
      localDiscovered[baseOffset+1] += (newVisits>>1) & byteOnes;
      localDiscovered[baseOffset+2] += (newVisits>>2) & byteOnes;
      localDiscovered[baseOffset+3] += (newVisits>>3) & byteOnes;
      localDiscovered[baseOffset+4] += (newVisits>>4) & byteOnes;
      localDiscovered[baseOffset+5] += (newVisits>>5) & byteOnes;
      localDiscovered[baseOffset+6] += (newVisits>>6) & byteOnes;
      localDiscovered[baseOffset+7] += (newVisits>>7) & byteOnes;

      if(localDiscoveredIteration[partIx]==254) {
         updateDiscoveredNodes(partIx);
         localDiscoveredIteration[partIx]=0;
      } else {
         localDiscoveredIteration[partIx]++;
      }
   }

   void finalize() {
      for (unsigned i = 0; i < width; ++i) {
         updateDiscoveredNodes(i);
      }
      memset(localDiscovered, 0, sizeof(bit_t)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }
};

template<uint64_t width>
struct BatchDistance<uint64_t, width> {
   uint32_t* numDistDiscovered;
   uint8_t localDiscoveredIteration[width];
   alignas(16) uint64_t localDiscovered[8*width];
   const uint64_t allZeros = 0;
   const uint64_t byteOnes = 0x0101010101010101ul;

   BatchDistance(uint32_t* numDistDiscovered) : numDistDiscovered(numDistDiscovered) {
      memset(localDiscovered, 0, sizeof(uint64_t)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }

   BatchDistance& operator=(BatchDistance&& other) = delete;

   void updateDiscoveredNodes(const uint64_t partIx) {
      for(unsigned a=0; a<8; a++) {
         const uint64_t localOffset = partIx*8+a;
         const uint64_t baseOffset = partIx*64+a;
         numDistDiscovered[baseOffset+8*0] += (localDiscovered[localOffset] >> 8*0) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*1] += (localDiscovered[localOffset] >> 8*1) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*2] += (localDiscovered[localOffset] >> 8*2) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*3] += (localDiscovered[localOffset] >> 8*3) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*4] += (localDiscovered[localOffset] >> 8*4) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*5] += (localDiscovered[localOffset] >> 8*5) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*6] += (localDiscovered[localOffset] >> 8*6) & 0x000000FFul;
         numDistDiscovered[baseOffset+8*7] += (localDiscovered[localOffset] >> 8*7) & 0x000000FFul;
      }
      for(int a=0; a<8; a++) {
         localDiscovered[partIx*8+a] = allZeros;
      }
   }

   inline void prefetch(uint64_t partIx) {
      __builtin_prefetch(&localDiscovered[partIx*8],1);
   }

   void updateDiscovered(uint64_t newVisits, uint64_t partIx) {
      assert(partIx<=width);
      uint64_t baseOffset = partIx*8;

      localDiscovered[baseOffset+0] += newVisits & byteOnes;
      localDiscovered[baseOffset+1] += (newVisits>>1) & byteOnes;
      localDiscovered[baseOffset+2] += (newVisits>>2) & byteOnes;
      localDiscovered[baseOffset+3] += (newVisits>>3) & byteOnes;
      localDiscovered[baseOffset+4] += (newVisits>>4) & byteOnes;
      localDiscovered[baseOffset+5] += (newVisits>>5) & byteOnes;
      localDiscovered[baseOffset+6] += (newVisits>>6) & byteOnes;
      localDiscovered[baseOffset+7] += (newVisits>>7) & byteOnes;

      if(localDiscoveredIteration[partIx]==254) {
         updateDiscoveredNodes(partIx);
         localDiscoveredIteration[partIx]=0;
      } else {
         localDiscoveredIteration[partIx]++;
      }
   }

   void finalize() {
      for (unsigned i = 0; i < width; ++i) {
         updateDiscoveredNodes(i);
      }
      memset(localDiscovered, 0, sizeof(uint64_t)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }
};

// SSE Variant
template<uint64_t width>
struct BatchDistance<__m128i, width> {
   uint32_t* numDistDiscovered;
   uint8_t localDiscoveredIteration[width];
   alignas(64) __m128i localDiscovered[8*width];

   const __m128i allZeros = _mm_set_epi32(0,0,0,0);
   const __m128i byteOnes = _mm_set_epi8(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);

   BatchDistance(uint32_t* numDistDiscovered) : numDistDiscovered(numDistDiscovered), localDiscoveredIteration() {
      memset(localDiscovered, 0, sizeof(__m128i)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }

   BatchDistance(BatchDistance<__m128i, width>&& other) = delete;
   //    : numDistDiscovered(other.numDistDiscovered), localDiscoveredIteration(move(other.localDiscoveredIteration)), localDiscovered(other.localDiscovered)
   // {
   //    other.localDiscovered = nullptr;
   // }
   BatchDistance(BatchDistance<__m128i, width>&) = delete;
   BatchDistance<__m128i, width>& operator=(BatchDistance<__m128i, width>&) = delete;

   // ~BatchDistance() {
   //    if(localDiscovered!=nullptr) {
   //       free(localDiscovered);
   //    }
   // }

   BatchDistance<__m128i, width>& operator=(BatchDistance<__m128i, width>&& other) = delete;
   // {
   //    numDistDiscovered = other.numDistDiscovered;
   //    localDiscoveredIteration = move(other.localDiscoveredIteration);
   //    localDiscovered = other.localDiscovered;
   //    other.localDiscovered = nullptr;
   // }

   void updateDiscoveredNodes(const uint64_t partIx) {
      for(unsigned a=0; a<8; a++) {
         uint32_t*__restrict const numDistDiscoveredBase = (uint32_t*)__builtin_assume_aligned(numDistDiscovered + partIx*128+a, 4);
         const auto localDiscoveredVal = localDiscovered[partIx*8+a];
         numDistDiscoveredBase[8*0 ] += _mm_extract_epi8(localDiscoveredVal, 0);
         numDistDiscoveredBase[8*1 ] += _mm_extract_epi8(localDiscoveredVal, 1);
         numDistDiscoveredBase[8*2 ] += _mm_extract_epi8(localDiscoveredVal, 2);
         numDistDiscoveredBase[8*3 ] += _mm_extract_epi8(localDiscoveredVal, 3);
         numDistDiscoveredBase[8*4 ] += _mm_extract_epi8(localDiscoveredVal, 4);
         numDistDiscoveredBase[8*5 ] += _mm_extract_epi8(localDiscoveredVal, 5);
         numDistDiscoveredBase[8*6 ] += _mm_extract_epi8(localDiscoveredVal, 6);
         numDistDiscoveredBase[8*7 ] += _mm_extract_epi8(localDiscoveredVal, 7);
         numDistDiscoveredBase[8*8 ] += _mm_extract_epi8(localDiscoveredVal, 8);
         numDistDiscoveredBase[8*9 ] += _mm_extract_epi8(localDiscoveredVal, 9);
         numDistDiscoveredBase[8*10] += _mm_extract_epi8(localDiscoveredVal, 10);
         numDistDiscoveredBase[8*11] += _mm_extract_epi8(localDiscoveredVal, 11);
         numDistDiscoveredBase[8*12] += _mm_extract_epi8(localDiscoveredVal, 12);
         numDistDiscoveredBase[8*13] += _mm_extract_epi8(localDiscoveredVal, 13);
         numDistDiscoveredBase[8*14] += _mm_extract_epi8(localDiscoveredVal, 14);
         numDistDiscoveredBase[8*15] += _mm_extract_epi8(localDiscoveredVal, 15);
      }
      __m128i*__restrict const localDiscoveredBase = (__m128i*)__builtin_assume_aligned(localDiscovered + partIx*8, 16);
      for(int a=0; a<8; a++) {
         localDiscoveredBase[a] = allZeros;
      }
   }

   inline void prefetch(uint64_t partIx) {
      __builtin_prefetch(&localDiscovered[partIx*8],1);
   }

   void updateDiscovered(__m128i newVisits, uint64_t partIx) {

      assert(partIx<=width);

      __m128i*__restrict const localDiscoveredBase = (__m128i*)__builtin_assume_aligned(localDiscovered+partIx*8, 16);
      localDiscoveredBase[0] = _mm_add_epi8(localDiscoveredBase[0], _mm_and_si128(newVisits, byteOnes));
      localDiscoveredBase[1] = _mm_add_epi8(localDiscoveredBase[1], _mm_and_si128(_mm_srli_epi64(newVisits, 1), byteOnes));
      localDiscoveredBase[2] = _mm_add_epi8(localDiscoveredBase[2], _mm_and_si128(_mm_srli_epi64(newVisits, 2), byteOnes));
      localDiscoveredBase[3] = _mm_add_epi8(localDiscoveredBase[3], _mm_and_si128(_mm_srli_epi64(newVisits, 3), byteOnes));
      localDiscoveredBase[4] = _mm_add_epi8(localDiscoveredBase[4], _mm_and_si128(_mm_srli_epi64(newVisits, 4), byteOnes));
      localDiscoveredBase[5] = _mm_add_epi8(localDiscoveredBase[5], _mm_and_si128(_mm_srli_epi64(newVisits, 5), byteOnes));
      localDiscoveredBase[6] = _mm_add_epi8(localDiscoveredBase[6], _mm_and_si128(_mm_srli_epi64(newVisits, 6), byteOnes));
      localDiscoveredBase[7] = _mm_add_epi8(localDiscoveredBase[7], _mm_and_si128(_mm_srli_epi64(newVisits, 7), byteOnes));

      if(localDiscoveredIteration[partIx]==254) {
         updateDiscoveredNodes(partIx);
         localDiscoveredIteration[partIx]=0;
      } else {
         localDiscoveredIteration[partIx]++;
      }
   }

   void finalize() {
      for (unsigned i = 0; i < width; ++i) {
         updateDiscoveredNodes(i);
      }
      memset(localDiscovered, 0, sizeof(__m128i)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }
};

// AVX2 Variant
#ifdef AVX2

template<uint64_t width>
struct BatchDistance<__m256i, width> {
   uint32_t* numDistDiscovered;
   uint8_t localDiscoveredIteration[width];
   alignas(64) __m256i localDiscovered[8*width];
   const __m256i allZeros = _mm256_set_epi32(0,0,0,0,0,0,0,0);
   const __m256i byteOnes = _mm256_set_epi8(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);

   BatchDistance(uint32_t* numDistDiscovered) : numDistDiscovered(numDistDiscovered), localDiscoveredIteration() {
      memset(localDiscovered, 0, sizeof(__m256i)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }
   BatchDistance(BatchDistance<__m256i, width>&) = delete;
   BatchDistance<__m256i, width>& operator=(BatchDistance<__m256i, width>&) = delete;

   // ~BatchDistance() {
   //    if(localDiscovered!=nullptr) {
   //       free(localDiscovered);
   //    }
   // }

   BatchDistance<__m256i, width>& operator=(BatchDistance<__m256i, width>&& other) = delete;
   //  {
   //    numDistDiscovered = other.numDistDiscovered;
   //    localDiscoveredIteration = move(other.localDiscoveredIteration);
   //    localDiscovered = other.localDiscovered;
   //    other.localDiscovered = nullptr;
   // }

   void updateDiscoveredNodes(const uint64_t partIx) {
      for(unsigned a=0; a<8; a++) {
         uint32_t*__restrict const numDistDiscoveredBase = (uint32_t*)__builtin_assume_aligned(numDistDiscovered + partIx*256+a, 4);
         const __m256i localDiscoveredVal = localDiscovered[partIx*8+a];
         numDistDiscoveredBase[8*0 ] += _mm256_extract_epi8(localDiscoveredVal, 0);
         numDistDiscoveredBase[8*1 ] += _mm256_extract_epi8(localDiscoveredVal, 1);
         numDistDiscoveredBase[8*2 ] += _mm256_extract_epi8(localDiscoveredVal, 2);
         numDistDiscoveredBase[8*3 ] += _mm256_extract_epi8(localDiscoveredVal, 3);
         numDistDiscoveredBase[8*4 ] += _mm256_extract_epi8(localDiscoveredVal, 4);
         numDistDiscoveredBase[8*5 ] += _mm256_extract_epi8(localDiscoveredVal, 5);
         numDistDiscoveredBase[8*6 ] += _mm256_extract_epi8(localDiscoveredVal, 6);
         numDistDiscoveredBase[8*7 ] += _mm256_extract_epi8(localDiscoveredVal, 7);
         numDistDiscoveredBase[8*8 ] += _mm256_extract_epi8(localDiscoveredVal, 8);
         numDistDiscoveredBase[8*9 ] += _mm256_extract_epi8(localDiscoveredVal, 9);
         numDistDiscoveredBase[8*10] += _mm256_extract_epi8(localDiscoveredVal, 10);
         numDistDiscoveredBase[8*11] += _mm256_extract_epi8(localDiscoveredVal, 11);
         numDistDiscoveredBase[8*12] += _mm256_extract_epi8(localDiscoveredVal, 12);
         numDistDiscoveredBase[8*13] += _mm256_extract_epi8(localDiscoveredVal, 13);
         numDistDiscoveredBase[8*14] += _mm256_extract_epi8(localDiscoveredVal, 14);
         numDistDiscoveredBase[8*15] += _mm256_extract_epi8(localDiscoveredVal, 15);
         numDistDiscoveredBase[8*16] += _mm256_extract_epi8(localDiscoveredVal, 16);
         numDistDiscoveredBase[8*17] += _mm256_extract_epi8(localDiscoveredVal, 17);
         numDistDiscoveredBase[8*18] += _mm256_extract_epi8(localDiscoveredVal, 18);
         numDistDiscoveredBase[8*19] += _mm256_extract_epi8(localDiscoveredVal, 19);
         numDistDiscoveredBase[8*20] += _mm256_extract_epi8(localDiscoveredVal, 20);
         numDistDiscoveredBase[8*21] += _mm256_extract_epi8(localDiscoveredVal, 21);
         numDistDiscoveredBase[8*22] += _mm256_extract_epi8(localDiscoveredVal, 22);
         numDistDiscoveredBase[8*23] += _mm256_extract_epi8(localDiscoveredVal, 23);
         numDistDiscoveredBase[8*24] += _mm256_extract_epi8(localDiscoveredVal, 24);
         numDistDiscoveredBase[8*25] += _mm256_extract_epi8(localDiscoveredVal, 25);
         numDistDiscoveredBase[8*26] += _mm256_extract_epi8(localDiscoveredVal, 26);
         numDistDiscoveredBase[8*27] += _mm256_extract_epi8(localDiscoveredVal, 27);
         numDistDiscoveredBase[8*28] += _mm256_extract_epi8(localDiscoveredVal, 28);
         numDistDiscoveredBase[8*29] += _mm256_extract_epi8(localDiscoveredVal, 29);
         numDistDiscoveredBase[8*30] += _mm256_extract_epi8(localDiscoveredVal, 30);
         numDistDiscoveredBase[8*31] += _mm256_extract_epi8(localDiscoveredVal, 31);
      }
      __m256i*__restrict const localDiscoveredBase = (__m256i*)__builtin_assume_aligned(localDiscovered + partIx*8, 32);
      for(int a=0; a<8; a++) {
         localDiscoveredBase[a] = allZeros;
      }
   }

   inline void prefetch(uint64_t partIx) {
      __builtin_prefetch(&localDiscovered[partIx*8],1);
   }

   void updateDiscovered(__m256i newVisits, uint64_t partIx) {

      assert(partIx<=width);

      __m256i*__restrict const localDiscoveredBase = (__m256i*)__builtin_assume_aligned(localDiscovered+partIx*8, 32);
      localDiscoveredBase[0] = _mm256_add_epi8(localDiscoveredBase[0], _mm256_and_si256(newVisits, byteOnes));
      localDiscoveredBase[1] = _mm256_add_epi8(localDiscoveredBase[1], _mm256_and_si256(_mm256_srli_epi64(newVisits, 1), byteOnes));
      localDiscoveredBase[2] = _mm256_add_epi8(localDiscoveredBase[2], _mm256_and_si256(_mm256_srli_epi64(newVisits, 2), byteOnes));
      localDiscoveredBase[3] = _mm256_add_epi8(localDiscoveredBase[3], _mm256_and_si256(_mm256_srli_epi64(newVisits, 3), byteOnes));
      localDiscoveredBase[4] = _mm256_add_epi8(localDiscoveredBase[4], _mm256_and_si256(_mm256_srli_epi64(newVisits, 4), byteOnes));
      localDiscoveredBase[5] = _mm256_add_epi8(localDiscoveredBase[5], _mm256_and_si256(_mm256_srli_epi64(newVisits, 5), byteOnes));
      localDiscoveredBase[6] = _mm256_add_epi8(localDiscoveredBase[6], _mm256_and_si256(_mm256_srli_epi64(newVisits, 6), byteOnes));
      localDiscoveredBase[7] = _mm256_add_epi8(localDiscoveredBase[7], _mm256_and_si256(_mm256_srli_epi64(newVisits, 7), byteOnes));

      if(localDiscoveredIteration[partIx]==254) {
         updateDiscoveredNodes(partIx);
         localDiscoveredIteration[partIx]=0;
      } else {
         localDiscoveredIteration[partIx]++;
      }
   }

   void finalize() {
      for (unsigned i = 0; i < width; ++i) {
         updateDiscoveredNodes(i);
      }
      memset(localDiscovered, 0, sizeof(__m256i)*8*width);
      memset(localDiscoveredIteration, 0, width);
   }
};
#endif

}