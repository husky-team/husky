#!/usr/bin/env python

import argparse
import os
import sys

husky_root = os.getenv('HUSKY_ROOT', '.')
os.chdir(husky_root)

def gen():
    cmd = 'doxygen {}/doxygen.config'.format(husky_root)
    os.system(cmd)

def get_server():
    python = sys.argv[0]
    if sys.version_info[0] == 3:
        return 'python -m http.server'
    elif sys.version_info[0] == 2:
        return 'python -m SimpleHTTPServer'
    else:
        raise 'Must use python >= 2.*'

def server():
    html_dir = husky_root + '/html'
    if not os.path.exists(html_dir):
        gen()
    print('Note: Enter CTRL-C to exit')
    cmd = 'cd {}/html && {}'.format(husky_root, get_server())
    os.system(cmd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Husky Doxygen Tool')
    parser.add_argument('-g', '--gen', help='Generate documentation', action='store_true')
    parser.add_argument('-s', '--server', help='Open a simple http server to view', action='store_true')

    args = parser.parse_args()
    if args.gen:
        gen()
    elif args.server:
        server()
    else:
        parser.print_help()
