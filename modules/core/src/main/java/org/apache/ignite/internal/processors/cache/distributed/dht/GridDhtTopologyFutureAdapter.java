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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.VOLATILE_DATA_REGION_NAME;

/**
 *
 */
public abstract class GridDhtTopologyFutureAdapter extends GridFutureAdapter<AffinityTopologyVersion>
    implements GridDhtTopologyFuture {
    /** Cache groups validation results. */
    protected volatile Map<Integer, CacheGroupValidation> grpValidRes = Collections.emptyMap();

    /** Whether or not cluster is active. */
    protected volatile boolean clusterIsActive = true;

    /**
     * @param grp Cache group.
     * @param topNodes Topology nodes.
     * @return Validation result.
     */
    protected final CacheGroupValidation validateCacheGroup(CacheGroupContext grp, Collection<ClusterNode> topNodes) {
        Collection<Integer> lostParts = grp.isLocal() ?
            Collections.<Integer>emptyList() : grp.topology().lostPartitions();

        boolean valid = true;

        if (!grp.systemCache()) {
            TopologyValidator validator = grp.topologyValidator();

            if (validator != null)
                valid = validator.validate(topNodes);
        }

        return new CacheGroupValidation(valid, lostParts);
    }

    /** {@inheritDoc} */
    @Override public final @Nullable CacheInvalidStateException validateCache(
        GridCacheContext cctx,
        boolean recovery,
        boolean read,
        @Nullable Object key,
        @Nullable Collection<?> keys
    ) {
        assert isDone() : this;

        Throwable err = error();

        if (err != null)
            return new CacheInvalidStateException(err);

        if (!clusterIsActive) {
            return new CacheInvalidStateException(
                    "Failed to perform cache operation (cluster is not activated): " + cctx.name());
        }

        if (cctx.cache() == null)
            return new CacheInvalidStateException(
                "Failed to perform cache operation (cache is stopped): " + cctx.name());

        CacheGroupContext grp = cctx.group();

        if (cctx.shared().readOnlyMode() && !read && !isSystemCache(cctx.name())
            && !VOLATILE_DATA_REGION_NAME.equals(cctx.group().dataRegion().config().getName())) {
            return new CacheInvalidStateException(
                    new IgniteClusterReadOnlyException("Failed to perform cache operation (cluster is in " +
                            "read-only mode) [cacheGrp=" + cctx.group().name() + ", cache=" + cctx.name() + ']'));
        }

        CacheGroupValidation validation = grpValidRes.get(grp.groupId());

        if (validation == null)
            return null;

        if (!read && !validation.isValid()) {
            return new CacheInvalidStateException("Failed to perform cache operation " +
                    "(cache topology is not valid): " + cctx.name());
        }

        PartitionLossPolicy lossPlc = grp.config().getPartitionLossPolicy();

        if (!read && (lossPlc == READ_ONLY_SAFE || lossPlc == READ_ONLY_ALL)) {
            return new CacheInvalidStateException(
                 "Failed to write to cache (cache is moved to a read-only state): " + cctx.name());
        }

        // Reads from any partition are allowed in recovery mode.
        if (read && recovery)
            return null;

        if (key == null && keys == null)
            return new CacheInvalidStateException("Failed to perform a cache operation " +
                    "(the cache has lost partitions [cacheGrp=" + cctx.group().name() + ", cache=" + cctx.name() + ']');

        if (key != null)
            return validate(cctx, key, validation.lostPartitions());

        if (keys != null) {
            for (Object key0 : keys) {
                final CacheInvalidStateException res = validate(cctx, key0, validation.lostPartitions());

                if (res != null)
                    return res;
            }
        }

        return null;
    }

    /**
     * @return {@code true} If any lost partitions was detected.
     */
    public boolean hasLostPartitions() {
        return grpValidRes.values().stream()
            .anyMatch(CacheGroupValidation::hasLostPartitions);
    }

    /**
     * Cache group validation result.
     */
    protected static class CacheGroupValidation {
        /** Topology validation result. */
        private final boolean valid;

        /** Lost partitions on this topology version. */
        private final Collection<Integer> lostParts;

        /**
         * @param valid Valid flag.
         * @param lostParts Lost partitions.
         */
        private CacheGroupValidation(boolean valid, Collection<Integer> lostParts) {
            this.valid = valid;
            this.lostParts = lostParts;
        }

        /**
         * @return True if valid, False if invalide.
         */
        public boolean isValid() {
            return valid;
        }

        /**
         * @return True if lost partition is present, False if not.
         */
        public boolean hasLostPartitions() {
            return !F.isEmpty(lostParts);
        }

        /**
         * @return Lost patition ID collection.
         */
        public Collection<Integer> lostPartitions() {
            return lostParts;
        }
    }

    /**
     * @param cctx Context.
     * @param key Key.
     * @param lostParts Lost partitions.
     */
    private CacheInvalidStateException validate(
        GridCacheContext cctx,
        Object key,
        Collection<Integer> lostParts
    ) {
        // TODO optimize for KeyCacheObject.
        final int part = cctx.affinity().partition(key);

        return lostParts.contains(part) ?
            new CacheInvalidStateException("Failed to execute the cache operation " +
            "(all partition owners have left the grid, partition data has been lost) [" +
            "cacheName=" + cctx.name() + ", partition=" + part + ", key=" + key + ']') : null;
    }
}
