# Copyright 2016 Husky Team
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


include_directories(${PROJECT_SOURCE_DIR} ${EXTERNAL_INCLUDE})

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

if(THRIFT_FOUND)
    include_directories(${THRIFT_INCLUDE_DIR})
    add_definitions(-DWITH_THRIFT)
endif(THRIFT_FOUND)

set(husky husky-lib husky-io husky-core husky-base) 

### Applications

# Aggregator
add_executable(Aggregator aggregator.cpp)
target_link_libraries(Aggregator ${husky})
target_link_libraries(Aggregator ${EXTERNAL_LIB})
husky_default_properties(Aggregator)

# Vector
add_executable(Vector vector.cpp)
target_link_libraries(Vector ${husky})
target_link_libraries(Vector ${EXTERNAL_LIB})
husky_default_properties(Vector)

# LabeledPoint
add_executable(LabeledPoint labeled_point.cpp)
target_link_libraries(LabeledPoint ${husky})
target_link_libraries(LabeledPoint ${EXTERNAL_LIB})
husky_default_properties(LabeledPoint)

# WordCountMR
add_executable(WordCountMR wc_mr.cpp)
target_link_libraries(WordCountMR ${husky})
target_link_libraries(WordCountMR ${EXTERNAL_LIB})
husky_default_properties(WordCountMR)

# WordCountMongo
add_executable(WordCountMongo wc_mr_mongo.cpp)
target_link_libraries(WordCountMongo ${husky})
target_link_libraries(WordCountMongo ${EXTERNAL_LIB})
husky_default_properties(WordCountMongo)

# WordCountFlume
add_executable(WordCountFlume wc_mr_flume.cpp)
target_link_libraries(WordCountFlume ${husky})
target_link_libraries(WordCountFlume ${EXTERNAL_LIB})
husky_default_properties(WordCountFlume)

# PI
add_executable(PI pi.cpp)
target_link_libraries(PI ${husky})
target_link_libraries(PI ${EXTERNAL_LIB})
husky_default_properties(PI)

# CC
add_executable(CC connected_components.cpp)
target_link_libraries(CC ${husky})
target_link_libraries(CC ${EXTERNAL_LIB})
husky_default_properties(CC)

# PIC
add_executable(PIC pic.cpp)
target_link_libraries(PIC ${husky})
target_link_libraries(PIC ${EXTERNAL_LIB})
husky_default_properties(PIC)

# PageRank
add_executable(PageRank pagerank.cpp)
target_link_libraries(PageRank ${husky})
target_link_libraries(PageRank ${EXTERNAL_LIB})
husky_default_properties(PageRank)

# PR_CC
add_executable(PR_CC pr_cc_attrlist.cpp)
target_link_libraries(PR_CC ${husky})
target_link_libraries(PR_CC ${EXTERNAL_LIB})
husky_default_properties(PR_CC)

# Linear Regression
add_executable(LinearRegression linear_regression.cpp)
target_link_libraries(LinearRegression ${husky})
target_link_libraries(LinearRegression ${EXTERNAL_LIB})
husky_default_properties(LinearRegression)

# Logistic Regression
add_executable(LogisticRegression logistic_regression.cpp)
target_link_libraries(LogisticRegression ${husky})
target_link_libraries(LogisticRegression ${EXTERNAL_LIB})
husky_default_properties(LogisticRegression)

# SVM
add_executable(SVM svm.cpp)
target_link_libraries(SVM ${husky})
target_link_libraries(SVM ${EXTERNAL_LIB})
husky_default_properties(SVM)

# tf-idf
add_executable(TF-IDF tfidf.cpp)
target_link_libraries(TF-IDF ${husky})
target_link_libraries(TF-IDF ${EXTERNAL_LIB})
husky_default_properties(TF-IDF)

# Affinity matrix
add_executable(AMAT affinity.cpp)
target_link_libraries(AMAT ${husky})
target_link_libraries(AMAT ${EXTERNAL_LIB})
husky_default_properties(AMAT)
