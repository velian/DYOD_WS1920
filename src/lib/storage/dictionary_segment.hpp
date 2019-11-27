#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "value_segment.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   *  Creates a Dictionary segment from a given value segment.
   * Todo(Julius): Dictionary segment are we narrowing the constructors functionality to much? 
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    auto segment = std::dynamic_pointer_cast<ValueSegment<T>>(base_segment);
    DebugAssert(segment, "Invalid base segment for dictionary segment");
    auto segment_values = segment->values();

    // create dictionary
    _dictionary = std::make_shared<std::vector<T>>(segment_values);
    std::sort(_dictionary->begin(), _dictionary->end());
    _dictionary->erase(std::unique(_dictionary->begin(), _dictionary->end()), _dictionary->end());

    // create attribute vector with minimal width
    if (unique_values_count() <= UINT8_MAX) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint8_t>>(segment->size());
    } else if (unique_values_count() <= UINT16_MAX) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint16_t>>(segment->size());
    } else {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint32_t>>(segment->size());
    }

    // fill attribute vector with valueIDs
    for (size_t position = 0; position < segment->size(); position++) {
      auto value_id = ValueID(std::distance(
          _dictionary->begin(), std::lower_bound(_dictionary->begin(), _dictionary->end(), segment_values[position])));
      _attribute_vector->set(position, value_id);
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    return value_by_value_id(ValueID(_attribute_vector->get(chunk_offset)));
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const { return value_by_value_id(ValueID(_attribute_vector->get(chunk_offset))); }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    throw std::runtime_error("Tried to call append() on immutable dictionary segment");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto lower_bound = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    if (lower_bound == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->begin(), lower_bound));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(static_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto upper_bound = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    if (upper_bound == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->begin(), upper_bound));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(static_cast<T>(value)); }

  ValueID find_in_dict(T value) const {
    auto upper_bound_ref = std::find(_dictionary->cbegin(), _dictionary->cend(), value);
    if (upper_bound_ref == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    auto x = upper_bound_ref - _dictionary->cbegin();
    return static_cast<ValueID>(x);
  }
  ValueID find_in_dict(const AllTypeVariant& value) const { return find_in_dict(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final {
    return _dictionary->size() * sizeof(T) + _attribute_vector->size() * _attribute_vector->width();
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
