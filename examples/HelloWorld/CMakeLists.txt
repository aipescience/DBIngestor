cmake_minimum_required (VERSION 2.6)
project (HelloWorld.x)
set(AIDIR "${PROJECT_SOURCE_DIR}/HelloWorld")

if(NOT DEFINED DBINGESTOR_LIBRARY_PATH)
       set(DBINGESTOR_LIBRARY_PATH "/usr/local/DBIngestor/lib")
endif()

if(NOT DEFINED DBINGESTOR_INCLUDE_PATH)
       set(DBINGESTOR_INCLUDE_PATH "/usr/local/DBIngestor/include")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/Modules/")

set(CMAKE_BUILD_TYPE Release)

include_directories ("${PROJECT_SOURCE_DIR}/HelloWorld")
include_directories ("${DBINGESTOR_INCLUDE_PATH}")
link_directories ("${DBINGESTOR_LIBRARY_PATH}")

if(MSVC)
set(CMAKE_CXX_FLAGS "/EHsc")
endif()

file(GLOB FILES_SRC "${AIDIR}/*.cpp")

#MESSAGE(STATUS "Dir: " ${DIDIR})

add_executable (HelloWorld.x ${FILES_SRC})

find_package (SQLITE3)
find_package (Mysql)
find_package (ODBC)

if(SQLITE3_FOUND)
        target_link_libraries(HelloWorld.x ${SQLITE3_LIBRARIES})
endif()

if(MYSQL_FOUND)
        target_link_libraries(HelloWorld.x ${MYSQL_LIBRARY})
endif()

if(ODBC_FOUND)
        target_link_libraries(HelloWorld.x ${ODBC_LIBRARIES})
endif()

#target_link_libraries(HelloWorld.x ${Boost_PROGRAM_OPTIONS_LIBRARY})

target_link_libraries(HelloWorld.x DBIngestor)


