#!/bin/bash

if [ "$CLANG_FORMAT" != "" ];
then
    CLANG_FORMAT=$CLANG_FORMAT
else
    CLANG_FORMAT=clang-format
fi

function check_all {
    echo "[INFO] Running clang-format for all files"
    if [ -z "$HUSKY_ROOT" ]
    then
        echo "[ERROR] HUSKY_ROOT variable unset"
        echo "[ERROR] clang-format exists"
        exit
    fi
    find $HUSKY_ROOT -name "*.cpp" -or -name "*.hpp" | xargs $CLANG_FORMAT -style=file -i
}

function check_single {
    echo "[INFO] Running clang-format for $1"
    $CLANG_FORMAT -style=file -i $1
}

if [ "$#" -ne 1 ]
then
    check_all
elif [ "$#" -ne 2 ]
then
    check_single $1
fi

