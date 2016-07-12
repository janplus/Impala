// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_RUNTIME_MINMAX_VALUE_H
#define IMPALA_RUNTIME_MINMAX_VALUE_H

#include "iostream"

namespace impala_udf {

template<class numtype>
struct MinMaxVal {
  numtype min;
  numtype max;
  MinMaxVal(numtype min = 0, numtype max = 0) : min(min), max(max) { }
  MinMaxVal(const MinMaxVal &other) : min(other.min), max(other.max) { }

  void SetMin(numtype min) { min = min; }
  void SetMax(numtype max) { max = max; }
  void SetMinMax(numtype min, numtype max) { min = min; max = max; }

  MinMaxVal& operator=(const MinMaxVal& other) {
    if (&other == this) return *this;
    this->min = other.min;
    this->max = other.max;
    return *this;
  }

  bool operator==(const MinMaxVal& other) const {
    return (other.min <= max && other.max >= min);
  }
  bool operator!=(const MinMaxVal& other) const {
    return (other.min > max || other.max < min);
  }
  bool operator>(const MinMaxVal& other) const {
    return max > other.min;
  }
  bool operator>=(const MinMaxVal& other) const {
    return max >= other.min;
  }
  bool operator<(const MinMaxVal& other) const {
    return min < other.max;
  }
  bool operator<=(const MinMaxVal& other) const {
    return min <= other.max;
  }

  bool operator==(const numtype& other) const {
    return (other >= min && other <= max);
  }
  bool operator!=(const numtype& other) const { return !(*this == other); }
  bool operator>(const numtype& other) const {
    return other < max;
  }
  bool operator>=(const numtype& other) const {
    return other <= max;
  }
  bool operator<(const numtype& other) const {
    return other > min;
  }
  bool operator<=(const numtype& other) const {
    return other >= min;
  }
};

}

#endif
