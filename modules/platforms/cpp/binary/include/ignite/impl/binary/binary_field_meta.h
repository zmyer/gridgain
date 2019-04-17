/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_IMPL_BINARY_BINARY_FIELD_META
#define _IGNITE_IMPL_BINARY_BINARY_FIELD_META

#include <stdint.h>

namespace ignite
{
    namespace binary
    {
        /* Forward declarations. */
        class BinaryRawWriter;
        class BinaryRawReader;
    }

    namespace impl
    {
        namespace binary
        {
            /**
             * Field metadata.
             */
            class BinaryFieldMeta
            {
            public:
                /**
                 * Default constructor.
                 */
                BinaryFieldMeta() :
                    typeId(0),
                    fieldId(0)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param typeId Type ID.
                 * @param fieldId Field IDs.
                 */
                BinaryFieldMeta(int32_t typeId, int32_t fieldId) :
                    typeId(typeId),
                    fieldId(fieldId)
                {
                    // No-op.
                }

                /**
                 * Get type ID.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const
                {
                    return typeId;
                }

                /** 
                 * Get field ID.
                 *
                 * @return Field ID.
                 */
                int32_t GetFieldId() const
                {
                    return fieldId;
                }

                /**
                 * Write to data stream.
                 *
                 * @param writer Writer.
                 */
                void Write(ignite::binary::BinaryRawWriter& writer) const;

                /**
                 * Read from data stream.
                 *
                 * @param reader reader.
                 */
                void Read(ignite::binary::BinaryRawReader& reader);

            private:
                /** Type ID. */
                int32_t typeId;

                /** Field ID. */
                int32_t fieldId;
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_FIELD_META