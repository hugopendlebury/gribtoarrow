set(CMAKE_VERBOSE_MAKEFILE ON)
cmake_minimum_required(VERSION 3.16)

project(gribtoarrow)
set (CMAKE_CXX_STANDARD 17)

find_package(Arrow REQUIRED)

add_subdirectory(pybind11)

INCLUDE_DIRECTORIES(/usr/local/include;
/opt/homebrew/Cellar/python@3.12/3.12.1/Frameworks/Python.framework/Versions/3.12/Headers;
/Users/hugo/Development/python/venv/lib/python3.12/site-packages/pyarrow/include)

LINK_DIRECTORIES(/usr/local/lib;/Users/hugo/.pyenv/versions/3.12.0/lib
/Users/hugo/Development/python/venv/lib/python3.12/site-packages/pyarrow/lib
/Users/hugo/Development/python/venv/lib/python3.12/site-packages/pyarrow/
/opt/homebrew/Cellar/apache-arrow/14.0.1_2/lib)

#Note the order is important here
#pybind11_add_moudle should be before target_link_libraries
pybind11_add_module(gribtoarrow "./pythonApi/grib_to_arrow.cpp" 
                            "./src/gribreader.cpp" 
                            "./src/gribmessage.cpp" 
                            "./src/gribmessageiterator.cpp"
                            "./src/griblocationdata.cpp"
                            "./src/arrowutils.cpp"
                            "./src/converter.cpp")


#This must be after pybind11_add_module
target_link_libraries(gribtoarrow PRIVATE Arrow::arrow_shared arrow_python eccodes PRIVATE python3.12 )
