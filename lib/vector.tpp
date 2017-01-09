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

#include <cassert>

template <typename T>
DenseVector<T> DenseVector<T>::operator-() const {
    DenseVector<T> res(*this);

    for (int i = 0; i < feature_num; i++) {
        res[i] *= -1;
    }

    return res;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator*(T c) const {
    DenseVector<T> res(feature_num);

    for (int i = 0; i < feature_num; i++) {
        res[i] = c * vec[i];
    }

    return res;
}

template <typename T>
DenseVector<T> DenseVector<T>::scalar_multiple_with_intcpt(T c) const {
    DenseVector<T> res(feature_num + 1);

    for (int i = 0; i < feature_num; i++) {
        res[i] = c * vec[i];
    }
    res[feature_num] = c;

    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator*=(T c) {
    for (int i = 0; i < feature_num; i++) {
        vec[i] *= c;
    }

    return *this;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator/(T c) const {
    DenseVector<T> res(feature_num);

    for (int i = 0; i < feature_num; i++) {
        res[i] = vec[i] / c;
    }

    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator/=(T c) {
    for (int i = 0; i < feature_num; i++) {
        vec[i] /= c;
    }

    return *this;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator+(const DenseVector<T>& b) const {
    assert(feature_num == b.feature_num);

    DenseVector<T> res(feature_num);

    for (int i = 0; i < feature_num; i++) {
        res[i] = vec[i] + b.vec[i];
    }

    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator+=(const DenseVector<T>& b) {
    assert(feature_num == b.feature_num);

    for (int i = 0; i < feature_num; i++) {
        vec[i] += b.vec[i];
    }

    return *this;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator+(const SparseVector<T>& b) const {
    DenseVector<T> res(*this);
    res += b;
    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator+=(const SparseVector<T>& b) {
    assert(feature_num == b.get_feature_num());

    for (const FeaValPair<T>& entry : b) {
        vec[entry.fea] += entry.val;
    }

    return *this;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator-(const DenseVector<T>& b) const {
    assert(feature_num == b.feature_num);

    DenseVector<T> res(feature_num);

    for (int i = 0; i < feature_num; i++) {
        res[i] = vec[i] - b.vec[i];
    }

    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator-=(const DenseVector<T>& b) {
    assert(feature_num == b.feature_num);

    for (int i = 0; i < feature_num; i++) {
        vec[i] -= b.vec[i];
    }

    return *this;
}

template <typename T>
DenseVector<T> DenseVector<T>::operator-(const SparseVector<T>& b) const {
    DenseVector<T> res(*this);
    res -= b;
    return res;
}

template <typename T>
DenseVector<T>& DenseVector<T>::operator-=(const SparseVector<T>& b) {
    assert(feature_num == b.get_feature_num());

    for (const FeaValPair<T>& entry : b.vec) {
        vec[entry.fea] -= entry.val;
    }

    return *this;
}

template <typename T>
T DenseVector<T>::dot(const DenseVector<T>& b) const {
    assert(feature_num == b.feature_num);

    T res = 0;

    for (int i = 0; i < feature_num; i++) {
        res += vec[i] * b.vec[i];
    }

    return res;
}

template <typename T>
T DenseVector<T>::dot(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num());

    T res = 0;

    for (const FeaValPair<T>& entry : b) {
        res += vec[entry.fea] * entry.val;
    }

    return res;
}

template <typename T>
T DenseVector<T>::dot_with_intcpt(const DenseVector<T>& b) const {
    assert(feature_num == b.feature_num + 1);

    T res = 0;

    for (int i = 0; i < feature_num - 1; i++) {
        res += vec[i] * b.vec[i];
    }
    res += vec[feature_num - 1];

    return res;
}

template <typename T>
T DenseVector<T>::dot_with_intcpt(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num() + 1);

    T res = 0;

    for (const FeaValPair<T>& entry : b) {
        res += vec[entry.fea] * entry.val;
    }
    res += vec[feature_num - 1];

    return res;
}

template <typename T>
T DenseVector<T>::sorted_euclid_dist(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num());

    T res = 0;

    auto b_it = b.begin();
    int b_fea = b_it->fea;

    for (int i = 0; i < feature_num; i++) {
        if (i != b_fea) {
            res += vec[i] * vec[i];
        } else {
            T diff = vec[i] - b_it->val;
            res += diff * diff;
            b_it++;
            b_fea = b_it->fea;
        }
    }

    return res;
}

template <typename T>
SparseVector<T> SparseVector<T>::operator-() const {
    SparseVector<T> res(*this);

    for (FeaValPair<T>& entry : res) {
        entry.val *= -1;
    }

    return res;
}

template <typename T>
SparseVector<T> SparseVector<T>::operator*(T c) const {
    SparseVector<T> res(*this);

    for (FeaValPair<T>& entry : res) {
        entry.val *= c;
    }

    return res;
}

template <typename T>
SparseVector<T> SparseVector<T>::scalar_multiple_with_intcpt(T c) const {
    SparseVector<T> res(feature_num + 1);

    for (const auto& entry : vec) {
        res.vec.emplace_back(entry.fea, c * entry.val);
    }
    res.vec.emplace_back(feature_num, c);

    return res;
}

template <typename T>
SparseVector<T>& SparseVector<T>::operator*=(T c) {
    for (FeaValPair<T>& entry : vec) {
        entry.val *= c;
    }

    return *this;
}

template <typename T>
SparseVector<T> SparseVector<T>::operator/(T c) const {
    SparseVector<T> res(*this);

    for (FeaValPair<T>& entry : res) {
        entry.val /= c;
    }

    return res;
}

template <typename T>
SparseVector<T>& SparseVector<T>::operator/=(T c) {
    for (FeaValPair<T>& entry : vec) {
        entry.val /= c;
    }

    return *this;
}

template <typename T>
DenseVector<T> SparseVector<T>::operator+(const SparseVector<T>& b) const {
    assert(feature_num == b.feature_num);

    DenseVector<T> res(*this);

    for (const auto& entry : b.vec) {
        res[entry.fea] += entry.val;
    }

    return res;
}

template <typename T>
DenseVector<T> SparseVector<T>::operator-(const DenseVector<T>& b) const {
    assert(feature_num == b.get_feature_num());

    DenseVector<T> res(std::move(-b));

    for (const FeaValPair<T>& entry : vec) {
        res[entry.fea] += entry.val;
    }

    return res;
}

template <typename T>
DenseVector<T> SparseVector<T>::operator-(const SparseVector<T>& b) const {
    assert(feature_num == b.feature_num);

    DenseVector<T> res(*this);

    for (const auto& entry : b.vec) {
        res[entry.fea] -= entry.val;
    }

    return res;
}

template <typename T>
T SparseVector<T>::dot_with_intcpt(const DenseVector<T>& b) const {
    assert(feature_num == b.get_feature_num() + 1);

    T res = 0;

    for (const auto& entry : vec) {
        if (entry.fea < feature_num - 1) {
            res += entry.val * vec[entry.fea];
        } else {
            res += entry.val;
        }
    }

    return res;
}

template <typename T>
T SparseVector<T>::sorted_dot(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num());

    T res = 0;

    auto it = begin(), b_it = b.begin();
    int fea = it->fea, b_fea = b_it->fea;

    while (it != end() && b_it != b.end()) {
        if (fea > b_fea) {
            b_it++;
            b_fea = b_it->fea;
        } else if (fea < b_fea) {
            it++;
            fea = it->fea;
        } else {
            res += it->val * b_it->val;
            it++;
            fea = it->fea;
            b_it++;
            b_fea = b_it->fea;
        }
    }

    return res;
}

template <typename T>
T SparseVector<T>::sorted_dot_with_intcpt(const DenseVector<T>& b) const {
    assert(feature_num == b.get_feature_num() + 1);

    T res = 0;

    auto it = begin();
    auto b_it = b.begin_feaval();
    int fea = it->fea;
    int b_fea = (*b_it).fea;
    while (it != end() && b_it != b.end_feaval()) {
        if (fea == b_fea) {
            res += it->val * (*b_it).val;
            it++;
            fea = it->fea;
            b_it++;
            b_fea = (*b_it).fea;
        } else {
            b_it++;
            b_fea = (*b_it).fea;
        }
    }

    while (it != end()) {
        if (fea == feature_num - 1) {
            res += it->val;
            break;
        }
        it++;
        fea = it->fea;
    }
    return res;
}

template <typename T>
T SparseVector<T>::sorted_dot_with_intcpt(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num() + 1);

    T res = 0;

    auto it = begin(), b_it = b.begin();
    int fea = it->fea, b_fea = b_it->fea;
    while (it != end() && b_it != b.end()) {
        if (fea > b_fea) {
            b_it++;
            b_fea = b_it->fea;
        } else if (fea < b_fea) {
            it++;
            fea = it->fea;
        } else {
            res += it->val * b_it->val;
            it++;
            fea = it->fea;
            b_it++;
            b_fea = b_it->fea;
        }
    }

    while (it != end()) {
        if (fea == feature_num - 1) {
            res += it->val;
            break;
        }
        it++;
        fea = it->fea;
    }

    return res;
}

template <typename T>
T SparseVector<T>::sorted_euclid_dist(const SparseVector<T>& b) const {
    assert(feature_num == b.get_feature_num());

    T res = 0;

    auto it = begin(), b_it = b.begin();
    int fea = it->fea, b_fea = b_it->fea;

    while (it != end() && b_it != b.end()) {
        if (fea > b_fea) {
            res += b_it->val * b_it->val;
            b_it++;
            b_fea = b_it->fea;
            continue;
        } else if (fea < b_fea) {
            res += it->val * it->val;
            it++;
            fea = it->fea;
            continue;
        } else {
            T diff = it->val - b_it->val;
            res += diff * diff;
            it++;
            fea = it->fea;
            b_it++;
            b_fea = b_it->fea;
        }
    }

    return res;
}
