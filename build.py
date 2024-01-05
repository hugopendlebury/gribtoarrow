from typing import Any, Dict
from pybind11.setup_helpers import Pybind11Extension, build_ext
import pyarrow
import site
import os
from pathlib import Path
import requests
import tarfile
import subprocess
from glob import glob
import platform
from enum import Enum

class OSEnv(Enum):
    LINUX = 1
    OSX = 2

def getSystem():
    os = platform.system().lower()
    if os == 'linux':
        return OSEnv.LINUX
    elif os == 'darwin':
        return OSEnv.OSX
    raise Exception("Unsupported Operating system")

def getCMakePath() -> str:
    #The poetry file specifies the cmake package which will install the latest version
    #of cmake into our site-packages folder. 
    for location in site.getsitepackages():
        packages = os.listdir(location)
        if 'cmake' in packages:
            return f"{location}{os.sep}cmake{os.sep}data{os.sep}bin{os.sep}cmake"
    raise FileNotFoundError("Unable to find cmake")

def get_working_dir() -> Path:
    return Path(__file__).resolve().parent

def get_temp_eccodes_path() -> Path:
    return get_working_dir() / "temp_eccodes"

def get_eccodes_include_path() -> Path:
    return get_temp_eccodes_path() / "include"

def get_eccodes_lib_path() -> str:
    p = get_temp_eccodes_path() / "lib"
    return str(p)

def get_build_dir() -> Path:
    return get_temp_eccodes_path() / "build"

def getEccodes() -> Path:
    print("Downloading eccodes")
    temp_eccodes_dir = get_temp_eccodes_path()
    build_dir = get_build_dir()
    if not temp_eccodes_dir.exists():
        temp_eccodes_dir.mkdir()
    if not build_dir.exists():
        build_dir.mkdir()
    save_file = temp_eccodes_dir / "eccodes.tar.gz"
    url = "https://confluence.ecmwf.int/download/attachments/45757960/eccodes-2.33.0-Source.tar.gz"
    responce = requests.get(url)
    if responce.status_code == 200:
        print("Saving eccodes")
        with open(save_file, 'wb') as f:
            f.write(responce.content)
            with tarfile.open(save_file) as tar:
                print("Exctracting eccodes")
                tar.extractall(temp_eccodes_dir)
            return temp_eccodes_dir / "eccodes-2.33.0-Source" 


def runCmd(cmd, args=None):
    print(f"Executing {cmd} with args {args}")
    executeCmd = [cmd] 
    if args is not None:
        executeCmd.extend(args)
    print(f"executeCmd =  {executeCmd}")
    process = subprocess.run(executeCmd, 
                     cwd=str(get_build_dir()),
                     stdout=subprocess.PIPE)
    
    print(f"Done with return code {process.returncode} {process.stdout}")

def buildEccodes():
    cmake_path = getCMakePath()
    eccodes_path = getEccodes()
    print(f"eccodes_path = {eccodes_path}")
    print(f"cmake_path =  {cmake_path}")
    cmake_args = [
        f"-DCMAKE_INSTALL_PREFIX={get_temp_eccodes_path()}", #install into our temp dir
        f"-DBUILD_SHARED_LIBS=OFF", #We only want a static lib
        f"-DENABLE_JPG=OFF",
        f"-DENABLE_EXAMPLES=OFF",
        f"-DENABLE_NETCDF=OFF", #Not bothered about installing this conversion tool
        f"-DENABLE_FORTRAN=OFF", #Not bothered about fortran support
        f"-DENABLE_MEMFS=ON", #Want to statically link and avoid having definition file on disk
        f"-DENABLE_BUILD_TOOLS=OFF", #Don't want the command line tools
    ]
    all_args = cmake_args
    all_args.extend([str(eccodes_path)])
    runCmd(cmake_path, all_args)
    runCmd("make")
    runCmd("ctest")
    runCmd("make", ["install"])



def build(setup_kwargs: Dict[str, Any]) -> None:

    if setup_kwargs.get("build_eccodes", True):
        buildEccodes()

    arrow_libs = [f"-L{lib}" for lib in pyarrow.get_library_dirs()]
    arrow_link = [f"-Wl,-rpath,{lib}" for lib in pyarrow.get_library_dirs()]

    #Note osx uses clang++ as the compiler which doesn't use the same as -Wl,-Bstatic 
    #for static linking. Test for compiler first ?
    #GCC version 9 or less want 2a as standard not 20
    os = getSystem()
    standard = '20' if os == OSEnv.OSX else '2a'

    compiler_args = ['-O3']
    additional_compiler_args = [] if os == OSEnv.OSX else ['-fpermissive']
    compiler_args.extend(additional_compiler_args)

    ext_modules = [
        Pybind11Extension(
            "gribtoarrow", 
            glob("pythonApi/*.cpp") + glob("src/*.cpp"),
            include_dirs=[".",  get_eccodes_include_path(), pyarrow.get_include()],
            extra_compile_args=compiler_args,
            extra_link_args= arrow_libs + arrow_link + [f"-L{get_eccodes_lib_path()}", 
                                                        '-leccodes_memfs', 
                                                        '-larrow_python'] ,
            language='c++',
            cxx_std=standard
        ),
    ]
    setup_kwargs.update({
        "ext_modules": ext_modules,
        "cmd_class": {"build_ext": build_ext},
        "zip_safe": False,
    })
