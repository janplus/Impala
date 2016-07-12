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

#include <gtest/gtest.h>
#include <sstream>
#include "runtime/minmax-value.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala_udf {

class MinMaxValueTest : public testing::Test {
};

TEST_F(MinMaxValueTest, Compare) {
  EXPECT_EQ(MinMaxVal<int8_t>(6, 12), 7);
  EXPECT_NE(MinMaxVal<int8_t>(6, 12), 5);
  EXPECT_LT(MinMaxVal<int8_t>(6, 12), 7);
  EXPECT_LE(MinMaxVal<int8_t>(6, 12), 7);
  EXPECT_GT(MinMaxVal<int8_t>(6, 12), 7);
  EXPECT_GE(MinMaxVal<int8_t>(6, 12), 7);

  EXPECT_EQ(MinMaxVal<int16_t>(35, 327), 35);
  EXPECT_NE(MinMaxVal<int16_t>(35, 327), 328);
  EXPECT_LT(MinMaxVal<int16_t>(35, 327), 36);
  EXPECT_LE(MinMaxVal<int16_t>(35, 327), 36);
  EXPECT_GT(MinMaxVal<int16_t>(35, 327), 326);
  EXPECT_GE(MinMaxVal<int16_t>(35, 327), 326);

  EXPECT_EQ(MinMaxVal<int32_t>(35, 147483647), 13724);
  EXPECT_NE(MinMaxVal<int32_t>(35, 147483647), 34);
  EXPECT_LT(MinMaxVal<int32_t>(35, 147483647), 13724);
  EXPECT_LE(MinMaxVal<int32_t>(35, 147483647), 13724);
  EXPECT_GT(MinMaxVal<int32_t>(35, 147483647), 13724);
  EXPECT_GE(MinMaxVal<int32_t>(35, 147483647), 13724);

  EXPECT_EQ(MinMaxVal<int64_t>(35, 223372036854775808L), 143445436);
  EXPECT_NE(MinMaxVal<int64_t>(35, 223372036854775808L), 34);
  EXPECT_LT(MinMaxVal<int64_t>(35, 223372036854775808L), 143445436);
  EXPECT_LE(MinMaxVal<int64_t>(35, 223372036854775808L), 143445436);
  EXPECT_GT(MinMaxVal<int64_t>(35, 223372036854775808L), 143445436);
  EXPECT_GE(MinMaxVal<int64_t>(35, 223372036854775808L), 143445436);

  EXPECT_EQ(MinMaxVal<float>(3.5, 32.7), 3.5);
  EXPECT_NE(MinMaxVal<float>(3.5, 32.7), 3.4);
  EXPECT_LT(MinMaxVal<float>(3.5, 32.7), 3.6);
  EXPECT_LE(MinMaxVal<float>(3.5, 32.7), 3.6);
  EXPECT_GT(MinMaxVal<float>(3.5, 32.7), 3.6);
  EXPECT_GE(MinMaxVal<float>(3.5, 32.7), 3.6);

  EXPECT_EQ(MinMaxVal<double>(3.5, 32.7), 3.5);
  EXPECT_NE(MinMaxVal<double>(3.5, 32.7), 3.4);
  EXPECT_LT(MinMaxVal<double>(3.5, 32.7), 3.6);
  EXPECT_LE(MinMaxVal<double>(3.5, 32.7), 3.6);
  EXPECT_GT(MinMaxVal<double>(3.5, 32.7), 3.6);
  EXPECT_GE(MinMaxVal<double>(3.5, 32.7), 3.6);
}

TEST_F(MinMaxValueTest, CompareMinMax) {
  EXPECT_EQ(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(7, 13));
  EXPECT_NE(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(2, 5));
  EXPECT_LT(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(7, 13));
  EXPECT_LE(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(7, 13));
  EXPECT_GT(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(7, 13));
  EXPECT_GE(MinMaxVal<int8_t>(6, 12), MinMaxVal<int8_t>(7, 13));

  EXPECT_EQ(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(326, 428));
  EXPECT_NE(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(328, 428));
  EXPECT_LT(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(326, 428));
  EXPECT_LE(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(326, 428));
  EXPECT_GT(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(326, 428));
  EXPECT_GE(MinMaxVal<int16_t>(35, 327), MinMaxVal<int16_t>(326, 428));

  EXPECT_EQ(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(13724, 14789));
  EXPECT_NE(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(3, 34));
  EXPECT_LT(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(13724, 14789));
  EXPECT_LE(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(13724, 14789));
  EXPECT_GT(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(13724, 14789));
  EXPECT_GE(MinMaxVal<int32_t>(35, 147483647), MinMaxVal<int32_t>(13724, 14789));

  EXPECT_EQ(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 36));
  EXPECT_NE(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 34));
  EXPECT_LT(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 36));
  EXPECT_LE(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 36));
  EXPECT_GT(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 36));
  EXPECT_GE(MinMaxVal<int64_t>(35, 223372036854775808L), MinMaxVal<int64_t>(1, 36));

  EXPECT_EQ(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(2.1, 40.7));
  EXPECT_NE(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(32.8, 40.7));
  EXPECT_LT(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(2.1, 40.7));
  EXPECT_LE(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(2.1, 40.7));
  EXPECT_GT(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(2.1, 40.7));
  EXPECT_GE(MinMaxVal<float>(3.5, 32.7), MinMaxVal<float>(2.1, 40.7));

  EXPECT_EQ(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 40.7));
  EXPECT_NE(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 3.4));
  EXPECT_LT(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 40.7));
  EXPECT_LE(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 40.7));
  EXPECT_GT(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 40.7));
  EXPECT_GE(MinMaxVal<double>(3.5, 32.7), MinMaxVal<double>(2.1, 40.7));
}

TEST_F(MinMaxValueTest, NotMatch) {
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) == 5));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) != 6));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) < 6));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) <= 5));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) > 12));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) >= 13));

  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) == 328));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) != 327));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) < 35));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) <= 34));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) > 327));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) >= 328));

  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) == 34));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) != 35));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) < 35));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) <= 34));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) > 147483647));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) >= 147483648));

  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) == 34));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) != 223372036854775808L));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) < 35));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) <= 34));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) > 223372036854775808L));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) >= 223372036854775809L));

  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) == 32.8));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) != 3.6));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) < 3.5));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) <= 3.4));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) > 32.7));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) >= 32.8));

  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) == 3.4));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) != 32.6));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) < 3.5));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) <= 3.4));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) > 32.7));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) >= 32.8));
}

TEST_F(MinMaxValueTest, NotMatchMinMax) {
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) == MinMaxVal<int8_t>(13, 15)));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) != MinMaxVal<int8_t>(12, 15)));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) < MinMaxVal<int8_t>(2, 6)));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) <= MinMaxVal<int8_t>(2, 5)));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) > MinMaxVal<int8_t>(12, 15)));
  EXPECT_FALSE((MinMaxVal<int8_t>(6, 12) >= MinMaxVal<int8_t>(13, 15)));

  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) == MinMaxVal<int16_t>(328, 428)));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) != MinMaxVal<int16_t>(3, 35)));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) < MinMaxVal<int16_t>(3, 35)));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) <= MinMaxVal<int16_t>(3, 34)));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) > MinMaxVal<int16_t>(327, 428)));
  EXPECT_FALSE((MinMaxVal<int16_t>(35, 327) >= MinMaxVal<int16_t>(328, 428)));

  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) == MinMaxVal<int32_t>(1, 34)));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) != MinMaxVal<int32_t>(1, 35)));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) < MinMaxVal<int32_t>(1, 35)));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) <= MinMaxVal<int32_t>(1, 34)));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) > MinMaxVal<int32_t>(147483647, 148000000)));
  EXPECT_FALSE((MinMaxVal<int32_t>(35, 147483647) >= MinMaxVal<int32_t>(147483648, 148000000)));

  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) == MinMaxVal<int64_t>(1, 34)));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) != MinMaxVal<int64_t>(1, 36)));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) < MinMaxVal<int64_t>(1, 35)));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) <= MinMaxVal<int64_t>(1, 34)));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) > MinMaxVal<int64_t>(223372036854775808L, 233372036854775808L)));
  EXPECT_FALSE((MinMaxVal<int64_t>(35, 223372036854775808L) >= MinMaxVal<int64_t>(223372036854775809L, 233372036854775808L)));

  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) == MinMaxVal<float>(32.8, 40.7)));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) != MinMaxVal<float>(32.7, 40.7)));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) < MinMaxVal<float>(2.1, 3.5)));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) <= MinMaxVal<float>(2.1, 3.4)));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) > MinMaxVal<float>(32.7, 40.7)));
  EXPECT_FALSE((MinMaxVal<float>(3.5, 32.7) >= MinMaxVal<float>(32.8, 40.7)));

  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) == MinMaxVal<double>(32.8, 40.7)));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) != MinMaxVal<double>(32.7, 40.7)));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) < MinMaxVal<double>(2.1, 3.5)));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) <= MinMaxVal<double>(2.1, 3.4)));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) > MinMaxVal<double>(32.7, 40.7)));
  EXPECT_FALSE((MinMaxVal<double>(3.5, 32.7) >= MinMaxVal<double>(32.8, 40.7)));
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
