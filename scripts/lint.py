#!/usr/bin/env python

import getopt
import os
import sys


CPPLINT_PY = os.path.dirname(os.path.abspath(__file__)) + '/cpplint.py'
CPPLINT_EXTENSIONS = ['cpp', 'hpp', 'tpp']
CPPLINT_FILTERS = ['-whitespace/indent', '-runtime/references', '+build/include_alpha', '-build/c++11', '-legal/copyright', '-readability/casting']
CPPLINT_LINE_LENGTH = 120

def usage():
    print "Run command as: $ lint.py PATH_DIRECTORY"

def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as err:
        print(err) # will print something like "option -a not recognized"
        usage()
        return 2

    for o, _ in opts:
        if o in ("-h", "--help"):
            usage()
            return 0

    if args is None:
        print "Need a directory path"
        usage()
        return 2


    find_files_cmd = "find " + ' '.join(args)
    cpplint_cmd = [
        CPPLINT_PY,
        '--linelength={}'.format(CPPLINT_LINE_LENGTH),
        '--extensions={}'.format(','.join(CPPLINT_EXTENSIONS)),
    ]
    if (len(CPPLINT_FILTERS) > 0):
        cpplint_cmd.append('--filter={}'.format(','.join(CPPLINT_FILTERS)))

    run_cmd = find_files_cmd + ' | xargs ' + ' '.join(cpplint_cmd)
    return  os.system(run_cmd)


if __name__ == '__main__':
    sys.exit(main())

