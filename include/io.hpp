//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <cstddef>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>

namespace io {
   /// Owner of a mapped file
   class MmapedFile {
      int fd;
   public:
      size_t size;
      void* mapping;

      MmapedFile(const std::string& path, int flags);
      MmapedFile(const MmapedFile&) = delete;
      MmapedFile(MmapedFile&&);
      ~MmapedFile();
   };

   size_t fileLines(const std::string& path);
}