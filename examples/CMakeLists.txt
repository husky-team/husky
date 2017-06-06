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


include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

set(husky husky-lib husky-io husky-core husky-base) 

### Applications

# Aggregator
add_executable(Aggregator aggregator.cpp)
target_link_libraries(Aggregator ${husky})
target_link_libraries(Aggregator ${HUSKY_EXTERNAL_LIB})
husky_default_properties(Aggregator)

# Vector
add_executable(Vector vector.cpp)
target_link_libraries(Vector ${husky})
target_link_libraries(Vector ${HUSKY_EXTERNAL_LIB})
husky_default_properties(Vector)

# VectorDeprecated
add_executable(VectorDeprecated vector_deprecated.cpp)
target_link_libraries(VectorDeprecated ${husky})
target_link_libraries(VectorDeprecated ${HUSKY_EXTERNAL_LIB})
husky_default_properties(VectorDeprecated)

# LabeledPoint
add_executable(LabeledPoint labeled_point.cpp)
target_link_libraries(LabeledPoint ${husky})
target_link_libraries(LabeledPoint ${HUSKY_EXTERNAL_LIB})
husky_default_properties(LabeledPoint)

# WordCountMR
add_executable(WordCountMR wc_mr.cpp)
target_link_libraries(WordCountMR ${husky})
target_link_libraries(WordCountMR ${HUSKY_EXTERNAL_LIB})
husky_default_properties(WordCountMR)

# WordCountES
add_executable(WordCountES wc_mr_elasticsearch.cpp)
target_link_libraries(WordCountES ${husky})
target_link_libraries(WordCountES ${HUSKY_EXTERNAL_LIB})
husky_default_properties(WordCountES)

# WordCountMongo
add_executable(WordCountMongo wc_mr_mongo.cpp)
target_link_libraries(WordCountMongo ${husky})
target_link_libraries(WordCountMongo ${HUSKY_EXTERNAL_LIB})
husky_default_properties(WordCountMongo)

# WordCountFlume
add_executable(WordCountFlume wc_mr_flume.cpp)
target_link_libraries(WordCountFlume ${husky})
target_link_libraries(WordCountFlume ${HUSKY_EXTERNAL_LIB})
husky_default_properties(WordCountFlume)

# WordCountORC
add_executable(WordCountORC wc_mr_orc.cpp)
target_link_libraries(WordCountORC ${husky})
target_link_libraries(WordCountORC ${HUSKY_EXTERNAL_LIB})
husky_default_properties(WordCountORC)

# PI
add_executable(PI pi.cpp)
target_link_libraries(PI ${husky})
target_link_libraries(PI ${HUSKY_EXTERNAL_LIB})
husky_default_properties(PI)

# CC
add_executable(CC connected_components.cpp)
target_link_libraries(CC ${husky})
target_link_libraries(CC ${HUSKY_EXTERNAL_LIB})
husky_default_properties(CC)

# PIC
add_executable(PIC pic.cpp)
target_link_libraries(PIC ${husky})
target_link_libraries(PIC ${HUSKY_EXTERNAL_LIB})
husky_default_properties(PIC)

# PageRank
add_executable(PageRank pagerank.cpp)
target_link_libraries(PageRank ${husky})
target_link_libraries(PageRank ${HUSKY_EXTERNAL_LIB})
husky_default_properties(PageRank)

# PR_CC
add_executable(PR_CC pr_cc_attrlist.cpp)
target_link_libraries(PR_CC ${husky})
target_link_libraries(PR_CC ${HUSKY_EXTERNAL_LIB})
husky_default_properties(PR_CC)

# Linear Regression
add_executable(LinearRegression linear_regression.cpp)
target_link_libraries(LinearRegression ${husky})
target_link_libraries(LinearRegression ${HUSKY_EXTERNAL_LIB})
husky_default_properties(LinearRegression)

# Logistic Regression
add_executable(LogisticRegression logistic_regression.cpp)
target_link_libraries(LogisticRegression ${husky})
target_link_libraries(LogisticRegression ${HUSKY_EXTERNAL_LIB})
husky_default_properties(LogisticRegression)

# SVM
add_executable(SVM svm.cpp)
target_link_libraries(SVM ${husky})
target_link_libraries(SVM ${HUSKY_EXTERNAL_LIB})
husky_default_properties(SVM)

# tf-idf
add_executable(TF-IDF tfidf.cpp)
target_link_libraries(TF-IDF ${husky})
target_link_libraries(TF-IDF ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TF-IDF)

# Affinity matrix
add_executable(AMAT affinity.cpp)
target_link_libraries(AMAT ${husky})
target_link_libraries(AMAT ${HUSKY_EXTERNAL_LIB})
husky_default_properties(AMAT)

# Triangle counting
add_executable(TriangleCounting triangle_counting.cpp)
target_link_libraries(TriangleCounting ${husky})
target_link_libraries(TriangleCounting ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TriangleCounting)

# Neuron Network for MNIST
add_executable(NN_MNIST nn_mnist.cpp)
target_link_libraries(NN_MNIST ${husky})
target_link_libraries(NN_MNIST ${HUSKY_EXTERNAL_LIB})
husky_default_properties(NN_MNIST)

# Cube
add_executable(Cube cube.cpp)
target_link_libraries(Cube ${husky})
target_link_libraries(Cube ${HUSKY_EXTERNAL_LIB})
husky_default_properties(Cube)
