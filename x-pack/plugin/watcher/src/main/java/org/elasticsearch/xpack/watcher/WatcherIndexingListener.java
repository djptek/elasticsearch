/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * This index listener ensures, that watches that are being indexed are put into the trigger service
 * Because the condition for this might change based on the shard allocation, this class is also a
 * cluster state listener
 *
 * Whenever a write operation to the current active watch index is made, this listener checks, if
 * the document should also be added to the local trigger service
 *
 */
final class WatcherIndexingListener implements IndexingOperationListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(WatcherIndexingListener.class);

    static final Configuration INACTIVE = new Configuration(null, Collections.emptyMap());

    private final WatchParser parser;
    private final Clock clock;
    private final TriggerService triggerService;
    private volatile Configuration configuration = INACTIVE;

    WatcherIndexingListener(WatchParser parser, Clock clock, TriggerService triggerService) {
        this.parser = parser;
        this.clock = clock;
        this.triggerService = triggerService;
    }

    // package private for testing
    Configuration getConfiguration() {
        return configuration;
    }

    // package private for testing
    void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * single watch operations that check if the local trigger service should trigger for this
     * concrete watch
     *
     * Watch parsing could be optimized, so that parsing only happens on primary and where the
     * shard is supposed to be put into the trigger service at some point, right no we dont care
     *
     * Note, we have to parse on the primary, because otherwise a failure on the replica when
     * parsing the watch would result in failing
     * the replica
     *
     * @param shardId   The shard id object of the document being processed
     * @param operation The index operation
     * @return          The index operation
     */
    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        if (isWatchDocument(shardId.getIndexName())) {
            ZonedDateTime now = Instant.ofEpochMilli(clock.millis()).atZone(ZoneOffset.UTC);
            try {
                Watch watch = parser.parseWithSecrets(operation.id(), true, operation.source(), now, XContentType.JSON,
                    operation.getIfSeqNo(), operation.getIfPrimaryTerm());
                ShardAllocationConfiguration shardAllocationConfiguration = configuration.localShards.get(shardId);
                if (shardAllocationConfiguration == null) {
                    logger.debug("no distributed watch execution info found for watch [{}] on shard [{}], got configuration for {}",
                            watch.id(), shardId, configuration.localShards.keySet());
                    return operation;
                }

                boolean shouldBeTriggered = shardAllocationConfiguration.shouldBeTriggered(watch.id());
                if (shouldBeTriggered) {
                    if (watch.status().state().isActive()) {
                        logger.debug("adding watch [{}] to trigger service", watch.id());
                        triggerService.add(watch);
                    } else {
                        logger.debug("removing watch [{}] to trigger service", watch.id());
                        triggerService.remove(watch.id());
                    }
                } else {
                    logger.debug("watch [{}] should not be triggered", watch.id());
                }
            } catch (IOException e) {
                throw new ElasticsearchParseException("Could not parse watch with id [{}]", e, operation.id());
            }

        }

        return operation;
    }

    /**
     * In case of a document related failure (for example version conflict), then clean up resources for a watch
     * in the trigger service.
     */
    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getResultType() == Engine.Result.Type.FAILURE) {
            assert result.getFailure() != null;
            postIndex(shardId, index, result.getFailure());
        }
    }

    /**
     * In case of an engine related error, we have to ensure that the triggerservice does not leave anything behind
     *
     * TODO: If the configuration changes between preindex and postindex methods and we add a
     *       watch, that could not be indexed
     * TODO: this watch might not be deleted from the triggerservice. Are we willing to accept this?
     * TODO: This could be circumvented by using a threadlocal  in preIndex(), that contains the
     *       watch and is cleared afterwards
     *
     * @param shardId   The shard id object of the document being processed
     * @param index     The index operation
     * @param ex        The exception occurred during indexing
     */
    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (isWatchDocument(shardId.getIndexName())) {
            logger.debug(() -> new ParameterizedMessage("removing watch [{}] from trigger", index.id()), ex);
            triggerService.remove(index.id());
        }
    }

    /**
     * If the index operation happened on a watcher shard and is of doc type watcher, we will
     * remove the watch id from the trigger service
     *
     * @param shardId   The shard id object of the document being processed
     * @param delete    The delete operation
     * @return          The delete operation
     */
    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        if (isWatchDocument(shardId.getIndexName())) {
            logger.debug("removing watch [{}] to trigger service via delete", delete.id());
            triggerService.remove(delete.id());
        }
        return delete;
    }

    /**
     * Check if a supplied index and document matches the current configuration for watcher
     *
     * @param index   The index to check for
     * @return true if this is a watch in the active watcher index, false otherwise
     */
    private boolean isWatchDocument(String index) {
        return configuration.isIndexAndActive(index);
    }

    /**
     * Listen for cluster state changes. This method will start, stop or reload the watcher
     * service based on cluster state information.
     * The method checks, if there are local watch indices up and running.
     *
     * @param event The ClusterChangedEvent class containing the current and new cluster state
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // if there is no master node configured in the current state, this node should not try to trigger anything, but consider itself
        // inactive. the same applies, if there is a cluster block that does not allow writes
        if (Strings.isNullOrEmpty(event.state().nodes().getMasterNodeId()) ||
                event.state().getBlocks().hasGlobalBlockWithLevel(ClusterBlockLevel.WRITE)) {
            configuration = INACTIVE;
            return;
        }

        if (event.state().nodes().getLocalNode().isDataNode() && event.metaDataChanged()) {
            try {
                IndexMetaData metaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, event.state().metaData());
                if (metaData == null) {
                    configuration = INACTIVE;
                } else {
                    checkWatchIndexHasChanged(metaData, event);
                }
            } catch (IllegalStateException e) {
                logger.error("error loading watches index: [{}]", e.getMessage());
                configuration = INACTIVE;
            }
        }
    }

    private void checkWatchIndexHasChanged(IndexMetaData metaData, ClusterChangedEvent event) {
        String watchIndex = metaData.getIndex().getName();
        ClusterState state = event.state();
        String localNodeId = state.nodes().getLocalNode().getId();
        RoutingNode routingNode = state.getRoutingNodes().node(localNodeId);

        // no local shards, exit early
        List<ShardRouting> localShardRouting = routingNode.shardsWithState(watchIndex, STARTED, RELOCATING);
        if (localShardRouting.isEmpty()) {
            configuration = INACTIVE;
        } else {
            reloadConfiguration(watchIndex, localShardRouting, event);
        }
    }

    /**
     * Reload the configuration if the alias pointing to the watch index was changed or
     * the index routing table for an index was changed
     *
     * @param watchIndex        Name of the concrete watches index pointing
     * @param localShardRouting List of local shards of that index
     * @param event             The cluster changed event containing the new cluster state
     */
    private void reloadConfiguration(String watchIndex, List<ShardRouting> localShardRouting,
                                     ClusterChangedEvent event) {
        // changed alias means to always read a new configuration
        boolean isAliasChanged = watchIndex.equals(configuration.index) == false;
        if (isAliasChanged || hasShardAllocationIdChanged(watchIndex, event.state())) {
            IndexRoutingTable watchIndexRoutingTable = event.state().routingTable().index(watchIndex);
            Map<ShardId, ShardAllocationConfiguration> ids = getLocalShardAllocationIds(localShardRouting, watchIndexRoutingTable);
            configuration = new Configuration(watchIndex, ids);
        }
    }

    /**
     * Check if the routing table has changed and local shards are affected
     *
     * @param watchIndex Name of the concrete watches index pointing
     * @param state      The new cluster state
     * @return           true if the routing tables has changed and local shards are affected
     */
    private boolean hasShardAllocationIdChanged(String watchIndex, ClusterState state) {
        List<ShardRouting> allStartedRelocatedShards = state.getRoutingTable().index(watchIndex).shardsWithState(STARTED);
        allStartedRelocatedShards.addAll(state.getRoutingTable().index(watchIndex).shardsWithState(RELOCATING));

        // exit early, when there are shards, but the current configuration is inactive
        if (allStartedRelocatedShards.isEmpty() == false && configuration == INACTIVE) {
            return true;
        }

        // check for different shard ids
        String localNodeId = state.nodes().getLocalNodeId();
        Set<ShardId> clusterStateLocalShardIds = state.getRoutingNodes().node(localNodeId)
                .shardsWithState(watchIndex, STARTED, RELOCATING).stream()
                .map(ShardRouting::shardId)
                .collect(Collectors.toSet());
        Set<ShardId> configuredLocalShardIds = new HashSet<>(configuration.localShards.keySet());
        Set<ShardId> differenceSet = Sets.difference(clusterStateLocalShardIds, configuredLocalShardIds);
        if (differenceSet.isEmpty() == false) {
            return true;
        }

        Map<ShardId, List<String>> shards = allStartedRelocatedShards.stream()
                .collect(Collectors.groupingBy(ShardRouting::shardId,
                        Collectors.mapping(sr -> sr.allocationId().getId(),
                        Collectors.toCollection(ArrayList::new))));

        // sort the collection, so we have a stable order
        shards.values().forEach(Collections::sort);

        // check for different allocation ids
        for (Map.Entry<ShardId, ShardAllocationConfiguration> entry : configuration.localShards.entrySet()) {
            if (shards.containsKey(entry.getKey()) == false) {
                return true;
            }

            Collection<String> allocationIds = shards.get(entry.getKey());
            if (allocationIds.equals(entry.getValue().allocationIds) == false) {
                return true;
            }
        }

        return false;
    }

    /**
     * This returns a mapping of the shard it to the index of the shard allocation ids in that
     * list. The idea here is to have a basis for consistent hashing in order to decide if a
     * watch needs to be triggered locally or on another system, when it is being indexed
     * as a single watch action.
     *
     * Example:
     * - ShardId(".watch", 0)
     * - all allocation ids sorted (in the cluster): [ "a", "b", "c", "d"]
     * - local allocation id: b (index position 1)
     * - then store the size of the allocation ids and the index position
     *   data.put(ShardId(".watch", 0), new Tuple(1, 4))
     */
    Map<ShardId, ShardAllocationConfiguration> getLocalShardAllocationIds(List<ShardRouting> localShards, IndexRoutingTable routingTable) {
        Map<ShardId, ShardAllocationConfiguration> data = new HashMap<>(localShards.size());

        for (ShardRouting shardRouting : localShards) {
            ShardId shardId = shardRouting.shardId();

            // find all allocation ids for this shard id in the cluster state
            List<String> allocationIds = routingTable.shard(shardId.getId()).getActiveShards()
                    .stream()
                    .map(ShardRouting::allocationId)
                    .map(AllocationId::getId)
                    .collect(Collectors.toList());

            // sort the list so it is stable
            Collections.sort(allocationIds);

            String allocationId = shardRouting.allocationId().getId();
            int idx = allocationIds.indexOf(allocationId);
            data.put(shardId, new ShardAllocationConfiguration(idx, allocationIds.size(), allocationIds));
        }

        return data;
    }

    /**
     * A helper class, that contains shard configuration per shard id
     */
    static final class Configuration {

        final Map<ShardId, ShardAllocationConfiguration> localShards;
        final boolean active;
        final String index;

        Configuration(String index, Map<ShardId, ShardAllocationConfiguration> localShards) {
            this.active = localShards.isEmpty() == false;
            this.index = index;
            this.localShards = Collections.unmodifiableMap(localShards);
        }

        /**
         * Find out, if the supplied index matches the current watcher configuration and the
         * current state is active
         *
         * @param index The name of the index to compare with
         * @return false if watcher is not active or the passed index is not the watcher index
         */
        public boolean isIndexAndActive(String index) {
            return active == true && index.equals(this.index);
        }
    }

    static final class ShardAllocationConfiguration {
        final int index;
        final int shardCount;
        final List<String> allocationIds;

        ShardAllocationConfiguration(int index, int shardCount, List<String> allocationIds) {
            this.index = index;
            this.shardCount = shardCount;
            this.allocationIds = allocationIds;
        }

        public boolean shouldBeTriggered(String id) {
            int hash = Murmur3HashFunction.hash(id);
            int shardIndex = Math.floorMod(hash, shardCount);
            return shardIndex == index;
        }
    }
}
