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
    }

    private var dataSize: Long = 0

    private val state =
        (stateStore.get(streamDescriptor) as MSSQLDirectLoaderStreamState?)
            ?: throw IllegalStateException("No state found for stream $streamDescriptor.")
    private val sqlBuilder = state.sqlBuilder
    private var connection = state.dataSource.connection.also { it.autoCommit = false }
    private val identityInsertEnabled =
        sqlBuilder.isForeignTable &&
            sqlBuilder.hasIdentityColumn(connection).also { hasIdentity ->
                if (hasIdentity) {
                    log.info {
                        "Foreign table has an identity column; enabling IDENTITY_INSERT for this session"
                    }
                    sqlBuilder.setIdentityInsert(connection, true)
                }
            }
    private var preparedStatement =
        connection.prepareStatement(state.sqlBuilder.getFinalTableInsertColumnHeader().trimIndent())
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

    private fun reconnect() {
        runCatching { preparedStatement.close() }
        runCatching { connection.close() }
        connection = state.dataSource.connection.also { it.autoCommit = false }
        if (identityInsertEnabled) {
            sqlBuilder.setIdentityInsert(connection, true)
        }
        preparedStatement =
            connection.prepareStatement(
                state.sqlBuilder.getFinalTableInsertColumnHeader().trimIndent()
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

        // If CDC is enabled, remove stale records
        if (sqlBuilder.hasCdc) {
            sqlBuilder.deleteCdc(connection)
        }

        connection.commit()
    }

    override fun close() {
        log.info { "Closing connection for batch $batch" }
        if (identityInsertEnabled) {
            runCatching { sqlBuilder.setIdentityInsert(connection, false) }
                .onFailure { log.warn(it) { "Failed to reset IDENTITY_INSERT" } }
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
