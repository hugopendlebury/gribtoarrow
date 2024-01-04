# build.py

from typing import Any, Dict

from pybind11.setup_helpers import Pybind11Extension, build_ext

import pyarrow

def build(setup_kwargs: Dict[str, Any]) -> None:

    arrow_libs = [f"-L{lib}" for lib in pyarrow.get_library_dirs()]
    arrow_link = [f"-Wl,-rpath,{lib}" for lib in pyarrow.get_library_dirs()]

    ext_modules = [
        Pybind11Extension(
            "gribtoarrow", 
            ["pythonApi/grib_to_arrow.cpp" 
            ,"src/gribreader.cpp" 
            ,"src/gribmessage.cpp" 
            ,"src/gribmessageiterator.cpp"
            ,"src/griblocationdata.cpp"
            ,"src/arrowutils.cpp"
            ,"src/converter.cpp"],
            include_dirs=[".", "", pyarrow.get_include()],
            extra_compile_args=['-O3'],
            #Arrow::arrow_shared arrow_python
            extra_link_args= arrow_libs + arrow_link + ['-L/usr/local/lib', '-Wl,-rpath,/usr/local/lib','-leccodes', '-larrow_python'] ,
            language='c++',
            cxx_std=20
        ),
    ]
    setup_kwargs.update({
        "ext_modules": ext_modules,
        "cmd_class": {"build_ext": build_ext},
        "zip_safe": False,
    })



