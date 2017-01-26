template <typename OtherDerived>
EIGEN_STRONG_INLINE auto dot(const SparseMatrixBase<OtherDerived>& other) const {
    return other.dot(*this);
}
