﻿/*
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

namespace Apache.Ignite.Core.Cache.Configuration
{
    /// <summary>
    /// Mode indicating how Ignite should wait for write replies from other nodes.
    /// </summary>
    public enum CacheWriteSynchronizationMode
    {
        /// <summary>
        /// Mode indicating that Ignite should wait for write or commit replies from all nodes.
        /// This behavior guarantees that whenever any of the atomic or transactional writes
        /// complete, all other participating nodes which cache the written data have been updated.
        /// </summary>
        FullSync,

        /// <summary>
        /// Flag indicating that Ignite will not wait for write or commit responses from participating nodes,
        /// which means that remote nodes may get their state updated a bit after any of the cache write methods
        /// complete, or after {@link Transaction#commit()} method completes.
        /// </summary>
        FullAsync,

        /// <summary>
        /// This flag only makes sense for {@link CacheMode#PARTITIONED} mode. When enabled, Ignite will wait 
        /// for write or commit to complete on primary node, but will not wait for backups to be updated.
        /// </summary>
        PrimarySync
    }
}