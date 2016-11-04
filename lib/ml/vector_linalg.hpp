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

#pragma once

#include <string>
#include <utility>
#include <vector>

namespace husky {

typedef std::vector<double> vec_double;
typedef std::vector<std::pair<int, double>> vec_sp;

// Override vector<double> operator

// Scaling vector
void operator*=(vec_double&, const double&);
void operator/=(vec_double&, const double&);
void operator/=(vec_double&, const vec_double&);
vec_double operator*(const vec_double&, const double&);
vec_double operator*(const double&, const vec_double&);
vec_double operator/(vec_double&, const double&);

// Inner product
double operator*(const vec_double&, const vec_double&);
// first vecotr with length n and the second with length n+1
// the (n+1)-th term of second one is intercept term
double dot_with_intcpt(const vec_double&, const vec_double&);

void operator+=(vec_double&, const vec_double&);
void operator-=(vec_double&, const vec_double&);
vec_double operator+(const vec_double&, const vec_double&);
vec_double operator-(const vec_double&, const vec_double&);

void operator+=(vec_double&, const vec_sp&);
void operator/=(vec_sp&, const vec_double&);

}  // namespace husky
