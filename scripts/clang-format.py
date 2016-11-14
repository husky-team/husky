#!/usr/bin/env python

import argparse
import os
import subprocess
import sys


cpp_suffix = '*.[cht]pp'
default_dirs = [
    'base',
    'core',
    'io',
    'lib',
    'master',
]
ignored_dirs = [
    'io/input/flume_connector',
]

husky_root = os.getenv('HUSKY_ROOT', '.')
os.chdir(husky_root)
clang_format = os.getenv('CLANG_FORMAT', 'clang-format')

def list_files(path=None):
    dirs = None
    if path is not None:
        if os.path.isdir(path):
            dirs = path
        else:
            if not os.path.exists(path):
                return None
            return [path]

    if dirs is None:
        dirs, files = default_dirs, []
        for d in dirs:
            cmd = 'find {} -name "{}"'.format(d, cpp_suffix)
            res = subprocess.check_output(cmd, shell=True).rstrip()
            files += res.split('\n')
        removes = []
        for f in files:
            for d in ignored_dirs:
                if f.find(d) != -1:
                    removes.append(f)
        for r in removes:
            files.remove(r)
        return files

    cmd = 'find {} -name {}'.format(dirs, cpp_suffix)
    res = subprocess.check_output(cmd, shell=True).rstrip()
    return res.split('\n')

def replace(files=None):
    if files is None:
        return

    for f in files:
        cmd = '{} -style=file -i {}'.format(clang_format, f)
        os.system(cmd)

def check(files=None):
    if files is None:
        return

    for f in files:
        cmd = '{} -style=file {} | diff -u {} -'.format(clang_format, f, f)
        os.system(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Husky Clang-Format Tool')
    parser.add_argument('-o', choices=['replace', 'check'], help='Replace for all c++ codes or check them only with clang-format')
    parser.add_argument('path', nargs='?', help='Path to a specific file or a directory')
    args = parser.parse_args()

    files = None
    if args.path:
        files = list_files(args.path)
    else:
        files = list_files()

    if files is None:
        print('[Error] Path {} does not exist'.format(args.path))
        parser.print_help()

    if args.o is None:
        print('[Error] Need input argument for -o'.format(args.path))
        parser.print_help()

    if args.o == 'replace':
        replace(files)
    elif args.o == 'check':
        check(files)
