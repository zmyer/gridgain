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

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;

import java.lang.reflect.Field;

/**
 * Accessor that deals with fields.
 */
public class QueryFieldAccessor implements QueryPropertyAccessor {
    /** Field to access. */
    private final Field fld;

    /** */
    public QueryFieldAccessor(Field fld) {
        fld.setAccessible(true);

        this.fld = fld;
    }

    /** {@inheritDoc} */
    @Override public Object getValue(Object obj) throws IgniteCheckedException {
        try {
            return fld.get(obj);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setValue(Object obj, Object newVal) throws IgniteCheckedException {
        try {
            fld.set(obj, newVal);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to set field value", e);
        }
    }

    /** {@inheritDoc} */
    @Override public String getPropertyName() {
        return fld.getName();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getType() {
        return fld.getType();
    }
}
