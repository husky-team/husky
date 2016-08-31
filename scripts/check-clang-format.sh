#!/bin/bash

find $HUSKY_ROOT -name "*.cpp" | xargs clang-format -style=file -output-replacements-xml | grep -c "<replacement "
find $HUSKY_ROOT -name "*.hpp" | xargs clang-format -style=file -output-replacements-xml | grep -c "<replacement "
