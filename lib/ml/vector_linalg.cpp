// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <utility>
#include <vector>

#include "lib/ml/vector_linalg.hpp"

namespace husky {

typedef std::vector<double> vec_double;
typedef std::vector<std::pair<int, double>> vec_sp;

// Override vector<double> operator
void operator*=(vec_double& va, const double& c) {
    int n = va.size();
    for (int i = 0; i < n; i++)
        va[i] *= c;
}

void operator/=(vec_double& va, const double& c) {
    int n = va.size();
    for (int i = 0; i < n; i++)
        va[i] /= c;
}

void operator/=(vec_double& va, const vec_double& vb) {
    int n = va.size();
    for (int i = 0; i < n; i++) {
        if (vb[i] != 0)
            va[i] /= vb[i];
    }
}

vec_double operator*(const vec_double& va, const double& c) {
    vec_double vc = va;
    vc *= c;
    return vc;
}

inline vec_double operator*(const double& c, const vec_double& va) { return va * c; }

vec_double operator/(vec_double& va, const double& c) {
    vec_double vb = va;
    vb /= c;
    return vb;
}

// Inner Product
double operator*(const vec_double& va, const vec_double& vb) {
    int n = va.size();
    double sum = 0.0;
    for (int i = 0; i < n; i++)
        sum += va[i] * vb[i];
    return sum;
}

double dot_with_intcpt(const vec_double& vx, const vec_double& pv) {
    int n = vx.size();
    double sum = 0.0;
    for (int i = 0; i < n; i++)
        sum += vx[i] * pv[i];
    sum += pv[n];
    return sum;
}

void operator+=(vec_double& va, const vec_double& vb) {
    int n = va.size();
    for (int i = 0; i < n; i++)
        va[i] += vb[i];
}

void operator-=(vec_double& va, const vec_double& vb) {
    int n = va.size();
    for (int i = 0; i < n; i++)
        va[i] -= vb[i];
}

vec_double operator+(vec_double& va, const vec_double& vb) {
    vec_double vc = va;
    vc += vb;
    return vc;
}

vec_double operator-(vec_double& va, const vec_double& vb) {
    vec_double vc = va;
    vc -= vb;
    return vc;
}

// sparse vector
void operator+=(vec_double& va, const vec_sp& vb) {
    int n = vb.size();
    for (int i = 0; i < n; i++)
        va[vb[i].first] += vb[i].second;
}

void operator/=(vec_sp& va, const vec_double& vb) {
    int n = va.size();
    for (int i = 0; i < n; i++) {
        if (vb[va[i].first] != 0) {
            va[i].second = va[i].second / vb[va[i].first];
        }
    }
}

}  // namespace husky
