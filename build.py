from typing import Any, Dict, List, Union, Callable
from pybind11.setup_helpers import Pybind11Extension, build_ext
import pyarrow
import site
import os
from pathlib import Path
import requests
import tarfile
import subprocess
from glob import glob
import shutil
from functools import wraps
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

def get_build_dir() -> Path:
    return get_working_dir() / "build"

def get_temp_path(child_path: str) -> Path:
    return get_working_dir() / child_path

def get_temp_eccodes_path() -> Path:
    return get_working_dir() / "temp_eccodes"

def get_temp_libaec_path() -> Path:
    return get_working_dir() / "temp_libaec"

def get_eccodes_include_path() -> Path:
    return get_temp_eccodes_path() / "include"

def check_is_lib_or_lib64() -> bool:
    return (get_temp_eccodes_path() / "lib").exists()

def get_lib_path(func: Callable, as_string: bool = False) -> Union[str, Path]:
    temp = func()
    temp = temp / "lib" if check_is_lib_or_lib64() else temp / "lib64"
    return str(temp) if as_string else temp

def get_eccodes_lib_path(as_string: bool = False) -> Union[str, Path]:
    return get_lib_path(get_temp_eccodes_path, as_string)

def get_eccodes_build_dir() -> Path:
    return get_temp_eccodes_path() / "build"

def mkPaths(paths: List[Path]):
    for path in paths:
        if not path.exists():
            path.mkdir()

def download_extract_tar(url: str, save_file_name: Path, temp_dir: Path) -> Path :
    print(f"Downloading from url {url}")
    responce = requests.get(url, verify=False)
    if responce.status_code == 200:
        print(f"Saving save_file_name")
        with open(save_file_name, 'wb') as f:
            f.write(responce.content)
            with tarfile.open(save_file_name) as tar:
                print("Extracting tar file")
                tar.extractall(temp_dir)
            extracted_dir = temp_dir / url.split("/")[-1].replace(".tar.gz","")
            print(f"Extracted dir is {extracted_dir}")
            return extracted_dir

def getEccodes() -> Path:
    print("Downloading eccodes")
    temp_dir = get_temp_eccodes_path()
    build_dir = get_eccodes_build_dir()
    mkPaths([temp_dir, build_dir])
    save_file = temp_dir / "eccodes.tar.gz"
    url = "https://confluence.ecmwf.int/download/attachments/45757960/eccodes-2.33.0-Source.tar.gz"
    return download_extract_tar(url, save_file, temp_dir)

def getLibaec() -> Path:
    print("Downloading libaec")
    temp_dir = get_temp_libaec_path()
    build_dir = temp_dir / "build"
    mkPaths([temp_dir, build_dir])
    save_file = temp_dir / "libaec.tar.gz"
    url = "https://gitlab.dkrz.de/k202009/libaec/-/archive/v1.1.2/libaec-v1.1.2.tar.gz"
    return download_extract_tar(url, save_file, temp_dir)

def runCmd(cmd, cwd:Path, args=None):
    print(f"Executing {cmd} with args {args}")
    executeCmd = [cmd] 
    if args is not None:
        executeCmd.extend(args)
    print(f"executeCmd =  {executeCmd}")
    process = subprocess.run(executeCmd, 
            cwd=str(cwd),
                     stdout=subprocess.PIPE)
    
    print(f"Done with return code {process.returncode} {process.stdout}")

def make_install(build_path, args):
    cmake_path = getCMakePath()
    runCmd(cmake_path, build_path, args)
    runCmd("make", build_path)
    runCmd("make", build_path, ["install"])

def build_libaec():

    libaec_path = getLibaec()
    cmake_args = [
        f"-DCMAKE_INSTALL_PREFIX={get_temp_path('temp_libaec')}", #install into our temp dir.
    ]
    all_args = cmake_args
    build_path = get_temp_path("temp_libaec") / "build"
    all_args.extend([str(libaec_path)])
    make_install(build_path, all_args)
    shutil.rmtree(build_path)
    mkPaths([build_path])
    make_install(build_path, [libaec_path])


def buildEccodes():
    cmake_path = getCMakePath()
    eccodes_path = getEccodes()
    cmake_args = [
        f"-DCMAKE_INSTALL_PREFIX={get_temp_eccodes_path()}", #install into our temp dir
        f"-DBUILD_SHARED_LIBS=ON", #We only want a static lib
        f"-DENABLE_JPG=OFF",
        f"-DENABLE_EXAMPLES=OFF",
        f"-DENABLE_NETCDF=OFF", #Not bothered about installing this conversion tool
        f"-DENABLE_FORTRAN=OFF", #Not bothered about fortran support
        f"-DENABLE_MEMFS=ON", #Want to statically link and avoid having definition file on disk
        f"-DENABLE_BUILD_TOOLS=OFF", #Don't want the command line tools
    ]
    all_args = cmake_args
    all_args.extend([str(eccodes_path)])
    runCmd(cmake_path, get_eccodes_build_dir(), all_args)
    runCmd("make", get_eccodes_build_dir())
    ctest_path = "/".join(cmake_path.split("/")[:-1]) 
    runCmd(f"{ctest_path}/ctest", get_eccodes_build_dir())
    runCmd("make", get_eccodes_build_dir(), ["install"])

def mk_wheel_dirs(paths):
    for path in paths:
        if not path.exists():
            path.mkdir()

def buildhook(func):
    @wraps(func)
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        result =  func(*args, **kwargs)
        print(f"EXTENSION WAS BUILT with result {result}")
        print("copying eccodes lib so they are bundled with the Wheel")
        lib_extension  = '*.dylib' if getSystem() == OSEnv.OSX else '*.so*'
        eccodes_libs = list(get_eccodes_lib_path(False).glob(lib_extension))
        build_dir = get_build_dir()
        build_lib_path = list(get_build_dir().glob("lib*"))[0]
        build_lib_grib_arrow_path = build_lib_path  / "gribtoarrow"
        for extension in build_lib_path.glob(lib_extension):
            shutil.copy(extension, build_lib_grib_arrow_path)
        wheel_lib_path_name = "lib" if check_is_lib_or_lib64() else "lib64"
        eccodes_wheel_paths = [build_lib_grib_arrow_path  / "eccodes", build_lib_grib_arrow_path  / wheel_lib_path_name]
        mk_wheel_dirs(eccodes_wheel_paths)
        eccodes_main_path, eccodes_memfs_path = eccodes_wheel_paths
        libaec_libs = list(get_lib_path(get_temp_libaec_path).glob(lib_extension))
        for f in libaec_libs: 
            shutil.copy(f, eccodes_memfs_path)
        for f in eccodes_libs: 
            shutil.copy(f, eccodes_main_path)
            if "memfs" in str(f):
                shutil.copy(f, eccodes_memfs_path)
        return result
    return with_logging

try:
    from setuptools import Extension as _Extension
    from setuptools.command.build_ext import build_ext as _build_ext
except ImportError:
    from distutils.command.build_ext import build_ext as _build_ext  # type: ignore[assignment]
    from distutils.extension import Extension as _Extension  # type: ignore[assignment]gg
   
original = _build_ext.build_extensions
@buildhook
def monkey(*args, **kwargs):
    print(f"Monkey patched function called with args {args}")
    original(*args, **kwargs)
    print ("Done calling original function")
    
_build_ext.build_extensions = monkey    



def build(setup_kwargs: Dict[str, Any]) -> None:

    #Note osx uses clang++ as the compiler which doesn't use the same as -Wl,-Bstatic
    #for static linking. Test for compiler first ?
    #GCC version 9 or less want 2a as standard not 20
    os = getSystem()
    standard = 20 if os == OSEnv.OSX else '2a' 
    compiler_args = ['-O3']
    additional_compiler_args = [] if os == OSEnv.OSX else ['-fpermissive']
    compiler_args.extend(additional_compiler_args)

    #remove the build dir it seems to cause caching
    build_dir = get_build_dir()
    if build_dir.exists():
        shutil.rmtree(build_dir)  

    if setup_kwargs.get("build_eccodes", True):
        build_libaec()
        buildEccodes()
 
    #Note rpath and origin eccodes we are telling the linker to look for dependencies in ./eccodes which will be a 
    #folder we create inside the wheel
    rpath_location = "@loader_path" if os == OSEnv.OSX else "$ORIGIN"
    eccodes_linker = [f"-L{get_eccodes_lib_path(True)}", f"-Wl,-rpath,{rpath_location}/eccodes", "-leccodes", "-leccodes_memfs"]
    arrow_libs = [f"-L{lib}" for lib in pyarrow.get_library_dirs()]
    arrow_link = [f"-Wl,-rpath,{lib}" for lib in pyarrow.get_library_dirs()]


    ext_modules = [
        Pybind11Extension(
            "gribtoarrow", 
            glob("pythonApi/*.cpp") + glob("src/*.cpp"),
            include_dirs=[".",  get_eccodes_include_path(), pyarrow.get_include()],
            extra_compile_args=compiler_args,
            extra_link_args = eccodes_linker + arrow_libs + arrow_link + [ 
                                                         "-larrow_python",
                                                        ], 
            language='c++',
            cxx_std=standard
        ),
    ]
    setup_kwargs.update({
        "ext_modules": ext_modules,
        "cmd_class": {"build_ext": build_ext},
        "zip_safe": False,
    })
