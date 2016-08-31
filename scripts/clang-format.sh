#!/bin/bash

find $HUSKY_ROOT -name "*.cpp" | xargs clang-format -style=file -i
find $HUSKY_ROOT -name "*.hpp" | xargs clang-format -style=file -i
