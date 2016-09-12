#!/bin/bash

if [ "$CLANG_FORMAT" != "" ];
then
    CLANG_FORMAT=$CLANG_FORMAT
else
    CLANG_FORMAT=clang-format
fi

find $HUSKY_ROOT -name "*.cpp" -or -name "*.hpp" | xargs $CLANG_FORMAT -style=file -i
