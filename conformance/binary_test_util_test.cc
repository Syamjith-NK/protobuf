// Protocol Buffers - Google's data interchange format
// Copyright 2025 Google LLC.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

#include "conformance/binary_test_util.h"

#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "google/protobuf/descriptor.h"

namespace google {
namespace protobuf {
namespace conformance {
namespace {

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAreArray;

// The legacy table; the test names ValidDataScalar.<TYPE>[i],
// ValidDataRepeated.<TYPE>.*, PrematureEof.<TYPE> and friends are generated
// from it in this order.
TEST(AllFieldTypesExceptGroupTest, LegacyTable) {
  EXPECT_THAT(
      AllFieldTypesExceptGroup(),
      ElementsAre(FieldDescriptor::TYPE_DOUBLE, FieldDescriptor::TYPE_FLOAT,
                  FieldDescriptor::TYPE_INT64, FieldDescriptor::TYPE_UINT64,
                  FieldDescriptor::TYPE_INT32, FieldDescriptor::TYPE_UINT32,
                  FieldDescriptor::TYPE_FIXED64, FieldDescriptor::TYPE_FIXED32,
                  FieldDescriptor::TYPE_SFIXED64,
                  FieldDescriptor::TYPE_SFIXED32, FieldDescriptor::TYPE_BOOL,
                  FieldDescriptor::TYPE_SINT32, FieldDescriptor::TYPE_SINT64,
                  FieldDescriptor::TYPE_STRING, FieldDescriptor::TYPE_BYTES,
                  FieldDescriptor::TYPE_ENUM, FieldDescriptor::TYPE_MESSAGE));
}

// Enumerates FieldDescriptor::Type independently of the table.
TEST(AllFieldTypesExceptGroupTest, IsEveryTypeButGroup) {
  std::vector<FieldDescriptor::Type> all_but_group;
  for (int i = 1; i <= FieldDescriptor::MAX_TYPE; ++i) {
    auto type = static_cast<FieldDescriptor::Type>(i);
    if (type != FieldDescriptor::TYPE_GROUP) all_but_group.push_back(type);
  }
  EXPECT_THAT(AllFieldTypesExceptGroup(),
              UnorderedElementsAreArray(all_but_group));
}

TEST(PackableFieldTypesTest, IsTheNumericBoolAndEnumTypesInTableOrder) {
  EXPECT_THAT(
      PackableFieldTypes(),
      ElementsAre(FieldDescriptor::TYPE_DOUBLE, FieldDescriptor::TYPE_FLOAT,
                  FieldDescriptor::TYPE_INT64, FieldDescriptor::TYPE_UINT64,
                  FieldDescriptor::TYPE_INT32, FieldDescriptor::TYPE_UINT32,
                  FieldDescriptor::TYPE_FIXED64, FieldDescriptor::TYPE_FIXED32,
                  FieldDescriptor::TYPE_SFIXED64,
                  FieldDescriptor::TYPE_SFIXED32, FieldDescriptor::TYPE_BOOL,
                  FieldDescriptor::TYPE_SINT32, FieldDescriptor::TYPE_SINT64,
                  FieldDescriptor::TYPE_ENUM));
}

TEST(LengthDelimitedFieldTypesTest, IsStringBytesMessage) {
  EXPECT_THAT(
      LengthDelimitedFieldTypes(),
      ElementsAre(FieldDescriptor::TYPE_STRING, FieldDescriptor::TYPE_BYTES,
                  FieldDescriptor::TYPE_MESSAGE));
}

TEST(LengthDelimitedFieldTypesTest,
     TogetherWithPackableCoversAllFieldTypesExceptGroup) {
  std::vector<FieldDescriptor::Type> both = PackableFieldTypes();
  for (FieldDescriptor::Type type : LengthDelimitedFieldTypes()) {
    both.push_back(type);
  }
  EXPECT_THAT(both, UnorderedElementsAreArray(AllFieldTypesExceptGroup()));
}

}  // namespace
}  // namespace conformance
}  // namespace protobuf
}  // namespace google
