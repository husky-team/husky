#!/bin/bash

if [ "$CLANG_FORMAT" != "" ];
then
    CLANG_FORMAT=$CLANG_FORMAT
else
    CLANG_FORMAT=clang-format
fi

find $HUSKY_ROOT -name "*.cpp" -or -name "*.hpp" -exec sh -c "$CLANG_FORMAT -style=file {} | diff -u {} -" \;
