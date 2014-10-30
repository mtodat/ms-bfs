//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/io.hpp"
#include "include/log.hpp"

#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

using namespace io;

MmapedFile::MmapedFile(const std::string& path, int flags) {
   fd = ::open(path.c_str(),flags);
   if (fd<0) { FATAL_ERROR("Could not open file. Missing \"/\" at end of path? " << path); }
   size = lseek(fd,0,SEEK_END);
   if (!(~size)) { ::close(fd); FATAL_ERROR("Could not get file size. " << path); }
   mapping = mmap(0,size,PROT_READ,MAP_PRIVATE,fd,0);
   if (!mapping) { ::close(fd); FATAL_ERROR("Could not memory map file. " << path); }
   if (mapping == (void*)(~((size_t)0))) { ::close(fd); FATAL_ERROR("Could not memory map file. (Maybe directory): " << path); }
}

MmapedFile::MmapedFile(MmapedFile&& other) : fd(other.fd), size(other.size), mapping(other.mapping) {
   other.fd = -1;
   other.size = 0;
   other.mapping = nullptr;
}

MmapedFile::~MmapedFile() {
   if(fd != -1) {
      munmap(mapping, size);
      ::close(fd);
   }
}

size_t io::fileLines(const std::string& path) {
   io::MmapedFile file(path, O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);

   char* data = reinterpret_cast<char*>(file.mapping);
   size_t numLines=0;
   for(size_t i=0; i<file.size; i++) {
      if(data[i]=='\n') {
         numLines++;
      }
   }
   if(data[file.size-1]!='\n') {
      numLines++;
   }

   return numLines;
}
