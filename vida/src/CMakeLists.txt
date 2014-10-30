cmake_minimum_required(VERSION 2.8)
set(CMAKE_BUILD_TYPE Release CACHE STRING "Set project build type.")
project(SIGMOD14Contest)

find_path(BOOST_ROOT boost)
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.42 COMPONENTS iostreams system thread timer chrono REQUIRED)
#find_package(OpenMP)
#if (OPENMP_FOUND)
#    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
#    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
#endif()
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse3 -msse4.1 -march=native")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse3 -msse4.1 -march=native")

include_directories(
  ${Boost_INCLUDE_DIR} 
)

set(SOURCES_COMMON 
  ## Common source files that are used by all queries
  query4.hpp
  )

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
set(LIBS_COMMON
  ## Common libraries that are used by all queries
  rt
)
endif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")

## add_executable(query4 query4.hpp ${SOURCES_COMMON})
## target_link_libraries(query4 ${Boost_LIBRARIES} ${LIBS_COMMON})

add_executable(query query.cpp ${SOURCES_COMMON})
target_link_libraries(query ${Boost_LIBRARIES} ${LIBS_COMMON})
