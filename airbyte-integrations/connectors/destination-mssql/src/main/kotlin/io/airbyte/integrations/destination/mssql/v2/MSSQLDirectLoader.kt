/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.write.DirectLoader
import io.airbyte.cdk.load.write.DirectLoaderFactory
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLIsNotConfiguredForBulkLoad
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.sql.SQLException

@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION", "kotlin coroutines")
class MSSQLDirectLoader(
    config: MSSQLConfiguration,
    stateStore: StreamStateStore<MSSQLStreamState>,
    private val streamDescriptor: DestinationStream.Descriptor,
    private val batch: Int,
    private val parent: MSSQLDirectLoaderFactory
) : DirectLoader {
    private val log = KotlinLogging.logger {}
    private val recordCommitBatchSize = config.batchEveryNRecords
    private val maxBatchDataSize = config.maxBatchSizeBytes

    private companion object {
        const val MAX_BATCH_ATTEMPTS = 5
        const val MAX_DEDUP_ATTEMPTS = 5
        const val STATE_WAIT_TIMEOUT_MS = 300_000L
        const val STATE_WAIT_POLL_MS = 100L
    }

    private var dataSize: Long = 0

    // Stream setup (which registers the loader state) runs concurrently with the
    // accumulator tasks; records can arrive before start() completes, so wait
    // briefly for the state instead of failing the batch.
    private val state = run {
        val deadline = System.currentTimeMillis() + STATE_WAIT_TIMEOUT_MS
        var s = stateStore.get(streamDescriptor) as MSSQLDirectLoaderStreamState?
        while (s == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(STATE_WAIT_POLL_MS)
            s = stateStore.get(streamDescriptor) as MSSQLDirectLoaderStreamState?
        }
        s ?: throw IllegalStateException("No state found for stream $streamDescriptor.")
    }
    private val sqlBuilder = state.sqlBuilder
    private var connection = state.dataSource.connection.also { it.autoCommit = false }

    /**
     * Fast path for user-managed tables with a dedup key: rows are bulk-inserted into a
     * per-batch scratch table in [SCRATCH_SCHEMA_NAME], then a single set-based
     * INSERT ... WHERE NOT EXISTS copies only missing keys into the target. This replaces a
     * per-record MERGE with one set operation per batch.
     */
    private val scratchTable: String? =
        if (sqlBuilder.usesScratchTable()) sqlBuilder.scratchTableName(batch) else null

    private val targetHasIdentity =
        sqlBuilder.isForeignTable && sqlBuilder.hasIdentityColumn(connection)

    // Scratch tables are created without the IDENTITY property, so the flag is only needed
    // on the target — either for the session (direct path) or around the final dedup insert
    // (scratch path).
    private val identityInsertEnabled =
        scratchTable == null &&
            targetHasIdentity.also { hasIdentity ->
                if (hasIdentity) {
                    log.info {
                        "Foreign table has an identity column; enabling IDENTITY_INSERT for this session"
                    }
                    sqlBuilder.setIdentityInsert(connection, true)
                }
            }

    init {
        scratchTable?.let {
            sqlBuilder.ensureScratchSchema(connection)
            sqlBuilder.createScratchTable(connection, it)
            // Commit the DDL so the scratch table survives connection reconnects.
            connection.commit()
        }
    }

    private var preparedStatement =
        connection.prepareStatement(
            (scratchTable?.let { sqlBuilder.getScratchInsertColumnHeader(it) }
                    ?: sqlBuilder.getFinalTableInsertColumnHeader())
                .trimIndent()
        )
    private val pendingRecords = ArrayList<DestinationRecordRaw>(recordCommitBatchSize)

    override suspend fun accept(
        record: DestinationRecordRaw,
    ): DirectLoader.DirectLoadResult {
        pendingRecords.add(record)

        // Periodically execute the batch to avoid too-large batches
        if (pendingRecords.size >= recordCommitBatchSize) {
            executeBatchSafely()
        }

        // Periodically complete the batch and ack underlying records.
        dataSize += record.serializedSizeBytes

        if (dataSize >= maxBatchDataSize) {
            finish()
            return DirectLoader.Complete
        }

        return DirectLoader.Incomplete
    }

    private fun isConnectionFailure(e: SQLException): Boolean =
        e.sqlState?.startsWith("08") == true ||
            e.message?.contains("Connection reset", ignoreCase = true) == true ||
            e.message?.contains("Connection is closed", ignoreCase = true) == true

    private fun isDeadlock(e: SQLException): Boolean =
        e.errorCode == 1205 || e.sqlState == "40001" ||
            e.message?.contains("deadlock", ignoreCase = true) == true

    private fun reconnect() {
        runCatching { preparedStatement.close() }
        runCatching { connection.close() }
        connection = state.dataSource.connection.also { it.autoCommit = false }
        scratchTable?.let {
            sqlBuilder.ensureScratchSchema(connection)
            sqlBuilder.createScratchTable(connection, it)
            connection.commit()
        }
        if (identityInsertEnabled) {
            sqlBuilder.setIdentityInsert(connection, true)
        }
        preparedStatement =
            connection.prepareStatement(
                (scratchTable?.let { sqlBuilder.getScratchInsertColumnHeader(it) }
                        ?: state.sqlBuilder.getFinalTableInsertColumnHeader())
                    .trimIndent()
            )
    }

    private fun executeBatchSafely() {
        // This is to prevent deadlock errors that will nuke the transaction.
        // TODO: Promote direct loader to use suspend functions so this can use a suspending mutex
        synchronized(parent) {
            repeat(MAX_BATCH_ATTEMPTS) { attempt ->
                try {
                    preparedStatement.clearBatch()
                    for (record in pendingRecords) {
                        sqlBuilder.populateStatement(
                            preparedStatement,
                            record,
                            sqlBuilder.insertTableSchema
                        )
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                    preparedStatement.clearBatch()
                    preparedStatement.clearParameters()
                    connection.commit()
                    pendingRecords.clear()
                    return
                } catch (e: SQLException) {
                    if (!isConnectionFailure(e) || attempt == MAX_BATCH_ATTEMPTS - 1) {
                        MSSQLErrorClassifier.rethrowClassified(e)
                    }
                    log.warn(e) {
                        "Batch write failed due to a connection failure; reconnecting (attempt ${attempt + 1}/$MAX_BATCH_ATTEMPTS)"
                    }
                    runCatching { reconnect() }
                        .onFailure { log.warn(it) { "Reconnect attempt failed" } }
                    Thread.sleep(2000L * (attempt + 1))
                }
            }
        }
    }

    override suspend fun finish() {
        log.info { "Finishing batch $batch for stream $streamDescriptor" }

        // Execute remaining records if any
        executeBatchSafely()
        preparedStatement.close()

        // Flush the scratch staging table into the target with insert-only dedup
        scratchTable?.let { commitScratchToTarget(it) }

        // If CDC is enabled, remove stale records
        if (sqlBuilder.hasCdc) {
            sqlBuilder.deleteCdc(connection)
        }

        connection.commit()
    }

    private fun commitScratchToTarget(scratchName: String) {
        var identityOn = false
        try {
            // Concurrent partition loaders issue anti-join inserts into the same
            // target table; serialize them and retry transient deadlocks.
            synchronized(parent) {
                repeat(MAX_DEDUP_ATTEMPTS) { attempt ->
                    try {
                        if (targetHasIdentity && !identityOn) {
                            sqlBuilder.setIdentityInsert(
                                connection,
                                sqlBuilder.outputSchema,
                                sqlBuilder.tableName,
                                true
                            )
                            identityOn = true
                        }
                        sqlBuilder.insertMissingFromScratch(connection, scratchName)
                        connection.commit()
                        return
                    } catch (e: SQLException) {
                        if (attempt == MAX_DEDUP_ATTEMPTS - 1 ||
                            (!isDeadlock(e) && !isConnectionFailure(e))
                        ) {
                            throw e
                        }
                        log.warn {
                            "Dedup insert failed transiently; retrying (attempt ${attempt + 1}/$MAX_DEDUP_ATTEMPTS)"
                        }
                        runCatching { connection.rollback() }
                        if (isConnectionFailure(e)) {
                            // Scratch rows are already committed and survive the
                            // reconnect; the identity flag is session-scoped and
                            // must be re-armed on the new connection.
                            runCatching { reconnect() }
                                .onFailure { log.warn(it) { "Reconnect attempt failed" } }
                            identityOn = false
                        }
                        Thread.sleep(2000L * (attempt + 1))
                    }
                }
            }
        } finally {
            if (identityOn) {
                runCatching {
                        sqlBuilder.setIdentityInsert(
                            connection,
                            sqlBuilder.outputSchema,
                            sqlBuilder.tableName,
                            false
                        )
                    }
                    .onFailure { log.warn(it) { "Failed to reset IDENTITY_INSERT" } }
            }
            runCatching { sqlBuilder.dropScratchTable(connection, scratchName) }
                .onFailure { log.warn(it) { "Failed to drop scratch table $scratchName" } }
            runCatching { connection.commit() }
        }
    }

    override fun close() {
        log.info { "Closing connection for batch $batch" }
        if (identityInsertEnabled) {
            runCatching { sqlBuilder.setIdentityInsert(connection, false) }
                .onFailure { log.warn(it) { "Failed to reset IDENTITY_INSERT" } }
        }
        // Best-effort cleanup if the batch failed before finish() could commit.
        scratchTable?.let {
            runCatching { sqlBuilder.dropScratchTable(connection, it) }
            runCatching { connection.commit() }
        }
        connection.close()
    }
}

@Singleton
@Requires(condition = MSSQLIsNotConfiguredForBulkLoad::class)
class MSSQLDirectLoaderFactory(
    val config: MSSQLConfiguration,
    val stateStore: StreamStateStore<MSSQLStreamState>,
) : DirectLoaderFactory<MSSQLDirectLoader> {
    private val log = KotlinLogging.logger {}

    override val inputPartitions: Int =
        config.numInputPartitions // Distribute work by stream, if interleaved
    override val maxNumOpenLoaders: Int = config.maxNumOpenLoaders

    private var batch: Int = 0
    override fun create(
        streamDescriptor: DestinationStream.Descriptor,
        part: Int
    ): MSSQLDirectLoader {
        log.info { "Creating query builder for batch $batch of stream $streamDescriptor" }

        return MSSQLDirectLoader(config, stateStore, streamDescriptor, batch++, this)
    }
}
