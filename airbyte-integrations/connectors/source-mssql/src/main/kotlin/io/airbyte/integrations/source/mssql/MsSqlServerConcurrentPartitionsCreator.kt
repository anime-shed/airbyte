/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql

import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.output.sockets.toJson
import io.airbyte.cdk.read.DefaultJdbcSharedState
import io.airbyte.cdk.read.DefaultJdbcStreamState
import io.airbyte.cdk.read.JdbcConcurrentPartitionsCreator
import io.airbyte.cdk.read.JdbcCursorPartition
import io.airbyte.cdk.read.JdbcNonResumablePartitionReader
import io.airbyte.cdk.read.JdbcPartitionFactory
import io.airbyte.cdk.read.JdbcPartitionsCreator
import io.airbyte.cdk.read.JdbcPartitionsCreatorFactory
import io.airbyte.cdk.read.JdbcResumablePartitionReader
import io.airbyte.cdk.read.JdbcSplittablePartition
import io.airbyte.cdk.read.MODE_PROPERTY
import io.airbyte.cdk.read.PartitionReader
import io.airbyte.cdk.read.Sample
import io.airbyte.cdk.read.SelectQuerier
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import javax.inject.Singleton
import kotlin.random.Random

/**
 * Concurrent partitions creator factory that wins over the CDK's secondary
 * [io.airbyte.cdk.read.JdbcConcurrentPartitionsCreatorFactory] bean so resumed cursor-incremental
 * reads checkpoint periodically.
 */
@Primary
@Singleton
@Requires(property = MODE_PROPERTY, value = "concurrent")
class MsSqlServerConcurrentPartitionsCreatorFactory(
    partitionFactory: MsSqlServerJdbcPartitionFactory,
) :
    JdbcPartitionsCreatorFactory<
        DefaultJdbcSharedState,
        DefaultJdbcStreamState,
        MsSqlServerJdbcPartition,
    >(partitionFactory) {

    override fun partitionsCreator(
        partition: MsSqlServerJdbcPartition
    ): JdbcPartitionsCreator<
        DefaultJdbcSharedState,
        DefaultJdbcStreamState,
        MsSqlServerJdbcPartition,
    > = MsSqlServerConcurrentPartitionsCreator(partition, partitionFactory)
}

/**
 * Identical to [JdbcConcurrentPartitionsCreator] except that a splittable partition which the
 * partition factory returns unsplit (e.g. a resumed cursor-incremental read) is read with a
 * [JdbcResumablePartitionReader]: chunked LIMIT queries emit the cursor position after every
 * chunk instead of only once at the end of the whole range, so a retry resumes from the last
 * committed checkpoint rather than re-reading everything.
 */
class MsSqlServerConcurrentPartitionsCreator(
    partition: MsSqlServerJdbcPartition,
    partitionFactory:
        JdbcPartitionFactory<
            DefaultJdbcSharedState,
            DefaultJdbcStreamState,
            MsSqlServerJdbcPartition,
        >,
) :
    JdbcConcurrentPartitionsCreator<
        DefaultJdbcSharedState,
        DefaultJdbcStreamState,
        MsSqlServerJdbcPartition,
    >(partition, partitionFactory) {

    private val log = KotlinLogging.logger {}

    override suspend fun run(): List<PartitionReader> {
        // Ensure that the cursor upper bound is known, if required.
        if (partition is JdbcCursorPartition<*>) {
            ensureCursorUpperBound()
            if (
                streamState.cursorUpperBound == null || streamState.cursorUpperBound?.isNull == true
            ) {
                log.info { "Maximum cursor column value query found that the table was empty." }
                return listOf(CheckpointOnlyPartitionReader())
            }
        }
        // Handle edge case where the table can't be sampled.
        if (!sharedState.withSampling) {
            log.warn {
                "Table cannot be read by concurrent partition readers because it cannot be sampled."
            }
            // TODO: adaptive fetchSize computation?
            return listOf(JdbcNonResumablePartitionReader(partition))
        }
        // Sample the table for partition split boundaries and for record byte sizes.
        val sample: Sample<Pair<OpaqueStateValue?, Long>> =
            collectSample { record: SelectQuerier.ResultRow ->
                val boundary: OpaqueStateValue? =
                    (partition as? JdbcSplittablePartition<*>)?.incompleteState(record)
                val rowByteSize: Long =
                    sharedState.rowByteSizeEstimator().apply(record.data.toJson())
                boundary to rowByteSize
            }
        if (sample.kind == Sample.Kind.EMPTY) {
            log.info { "Sampling query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        val rowByteSizeSample: Sample<Long> = sample.map { (_, rowByteSize: Long) -> rowByteSize }
        streamState.fetchSize = sharedState.jdbcFetchSizeEstimator().apply(rowByteSizeSample)
        val expectedTableByteSize: Long = rowByteSizeSample.sampledValues.sum() * sample.valueWeight
        log.info { "Table memory size estimated at ${expectedTableByteSize shr 20} MiB." }
        // Handle edge case where the table can't be split.
        val splittable = partition as? JdbcSplittablePartition<*>
        if (splittable == null) {
            log.warn {
                "Table cannot be read by concurrent partition readers because it cannot be split."
            }
            return listOf(JdbcNonResumablePartitionReader(partition))
        }
        // Happy path.
        log.info { "Target partition size is ${sharedState.targetPartitionByteSize shr 20} MiB." }
        val secondarySamplingRate: Double =
            if (expectedTableByteSize <= sharedState.targetPartitionByteSize) {
                0.0
            } else {
                val expectedPartitionByteSize: Long =
                    expectedTableByteSize / sharedState.maxSampleSize
                if (expectedPartitionByteSize < sharedState.targetPartitionByteSize) {
                    expectedPartitionByteSize.toDouble() / sharedState.targetPartitionByteSize
                } else {
                    1.0
                }
            }
        val random = Random(expectedTableByteSize) // RNG output is repeatable.
        val splitBoundaries: List<OpaqueStateValue> =
            sample.sampledValues
                .filter { random.nextDouble() < secondarySamplingRate }
                .mapNotNull { (splitBoundary: OpaqueStateValue?, _) -> splitBoundary }
                .distinct()

        // Handle edge case with empty split boundaries when sampling rate is too low,
        // causing random filtering to discard all sampled boundaries, which would
        // lead to division by zero the in the split() function. Fall back to single partition.
        if (splitBoundaries.isEmpty()) {
            log.warn { "No split boundaries found, using single partition" }
            return listOf(JdbcResumablePartitionReader(splittable))
        }
        val partitions: List<MsSqlServerJdbcPartition> =
            partitionFactory.split(partition, splitBoundaries)
        if (partitions.size <= 1) {
            // Splittable but not split (e.g. resumed cursor-incremental range): read it
            // resumably so the cursor checkpoints after every chunk.
            log.info {
                "Partition was not split; using resumable partition reader" +
                    " for periodic checkpoints"
            }
            return listOf(JdbcResumablePartitionReader(splittable))
        }
        log.info { "Table will be read by ${partitions.size} concurrent partition reader(s)." }
        return partitions.map { JdbcNonResumablePartitionReader(it) }
    }
}
