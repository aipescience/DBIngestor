cmake_minimum_required (VERSION 2.6)
project (DBIngestor)
set(DIDIR "${PROJECT_SOURCE_DIR}/DBIngestor")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/Modules/")

############################################################################
## SET THIS TO THE MYSQL BASE PATH IF IT IS NOT AT A STANDARD PLACE
############################################################################
set(MYSQL_BASE_PATH "/usr/local/mysql")

set(CMAKE_BUILD_TYPE Release)
set(SQLITE3_BUILD_IFFOUND 1)
set(MYSQL_BUILD_IFFOUND 1)
set(ODBC_BUILD_IFFOUND 1)

set(_DEFAULT_INCLUDE_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/include")
set(_DEFAULT_LIBRARY_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib")
if (EXISTS "${CMAKE_INSTALL_PREFIX}/lib32/" AND CMAKE_SIZEOF_VOID_P EQUAL 4)
	set(_DEFAULT_LIBRARY_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib32")
elif (EXISTS "${CMAKE_INSTALL_PREFIX}/lib64/" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
	set(_DEFAULT_LIBRARY_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib64")
endif()

set(LIBRARY_INSTALL_DIR "${_DEFAULT_LIBRARY_INSTALL_DIR}" CACHE PATH "Install directory for library files")
set(INCLUDE_INSTALL_DIR "${_DEFAULT_INCLUDE_INSTALL_DIR}" CACHE PATH "Install directory for header files")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include_directories ("${PROJECT_SOURCE_DIR}/DBIngestor")

if(MSVC)
set(CMAKE_CXX_FLAGS "/EHsc")
endif()

file(GLOB FILES_SRC "${DIDIR}/*.h" "${DIDIR}/*.cpp" "${DIDIR}/Asserters/*.h" "${DIDIR}/Asserters/*.cpp" "${DIDIR}/Converters/*.h" "${DIDIR}/Converters/*.cpp" "${AIDIR}/*.h" "${AIDIR}/*.cpp")
file(GLOB HEADERS "${DIDIR}/*.h")

#MESSAGE(STATUS "Dir: " ${DIDIR})

SET(Boost_USE_MULTITHREAD ON)
find_package (Boost COMPONENTS program_options REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})

find_package (SQLITE3)
message("Found SQLITE3: ${SQLITE3_FOUND}")
if(SQLITE3_FOUND AND SQLITE3_BUILD_IFFOUND)
	include_directories(${SQLITE3_INCLUDE_DIR})
	add_definitions(-DDB_SQLITE3)
	set(FILES_SRC ${FILES_SRC} "${DIDIR}/DBAdaptors/DBSqlite3.cpp" "${DIDIR}/DBAdaptors/DBSqlite3.h")
endif()

find_package (Mysql)
message("Found MySQL: ${MYSQL_FOUND}")
if(MYSQL_FOUND AND MYSQL_BUILD_IFFOUND)
	include_directories(${MYSQL_INCLUDE_DIR})
	add_definitions(-DDB_MYSQL)
	set(FILES_SRC ${FILES_SRC} "${DIDIR}/DBAdaptors/DBMySQL.cpp" "${DIDIR}/DBAdaptors/DBMySQL.h")
endif()

find_package (ODBC)
message("Found ODBC: ${ODBC_FOUND}")
if(ODBC_FOUND AND ODBC_BUILD_IFFOUND)
	include_directories(${ODBC_INCLUDE_DIRS})
	add_definitions(-DDB_ODBC)
	set(FILES_SRC ${FILES_SRC} "${DIDIR}/DBAdaptors/DBODBC.cpp" "${DIDIR}/DBAdaptors/DBODBC.h")
        set(FILES_SRC ${FILES_SRC} "${DIDIR}/DBAdaptors/DBODBCBulk.cpp" "${DIDIR}/DBAdaptors/DBODBCBulk.h")
endif()

add_library (DBIngestor ${FILES_SRC})

if(SQLITE3_FOUND AND SQLITE3_BUILD_IFFOUND)
        target_link_libraries(DBIngestor ${SQLITE3_LIBRARIES})
endif()

if(MYSQL_FOUND AND MYSQL_BUILD_IFFOUND)
        target_link_libraries(DBIngestor ${MYSQL_LIBRARY})
endif()

if(ODBC_FOUND AND ODBC_BUILD_IFFOUND)
        target_link_libraries(DBIngestor ${ODBC_LIBRARIES})
endif()

target_link_libraries(DBIngestor ${Boost_PROGRAM_OPTIONS_LIBRARY})

INSTALL(TARGETS DBIngestor DESTINATION "${_DEFAULT_LIBRARY_INSTALL_DIR}")
INSTALL(FILES ${HEADERS} DESTINATION "${_DEFAULT_INCLUDE_INSTALL_DIR}")




