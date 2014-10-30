//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <immintrin.h>

#ifdef __AVX2__
#define AVX2 1
#endif

extern __m128i sseMasks[128];

#ifdef AVX2
extern __m256i sseMasks256[256];
#endif