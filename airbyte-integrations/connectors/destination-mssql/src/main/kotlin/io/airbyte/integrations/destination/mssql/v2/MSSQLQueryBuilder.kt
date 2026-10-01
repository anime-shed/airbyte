/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import com.microsoft.sqlserver.jdbc.SQLServerException
import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.command.SoftDelete
import io.airbyte.cdk.load.command.Update
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.BooleanValue
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.DateValue
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.ObjectValue
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimeWithTimezoneValue
import io.airbyte.cdk.load.data.TimeWithoutTimezoneValue
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampWithTimezoneValue
import io.airbyte.cdk.load.data.TimestampWithoutTimezoneValue
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_EXTRACTED_AT
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_GENERATION_ID
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_META
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.integrations.destination.mssql.v2.convert.AirbyteTypeToMssqlType
import io.airbyte.integrations.destination.mssql.v2.convert.AirbyteValueToStatement.Companion.setAsNullValue
import io.airbyte.integrations.destination.mssql.v2.convert.MSSQLValueCoercer
import io.airbyte.integrations.destination.mssql.v2.convert.MssqlType
import io.airbyte.integrations.destination.mssql.v2.convert.ResultSetToAirbyteValue.Companion.getAirbyteNamedValue
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.ResultSet

private val logger = KotlinLogging.logger {}

fun <T> String.executeQuery(connection: Connection, vararg args: String, f: (ResultSet) -> T): T {
    logger.debug { "EXECUTING SQL:\n$this" }

    connection.prepareStatement(this.trimIndent()).use { statement ->
        args.forEachIndexed { index, arg -> statement.setString(index + 1, arg) }
        return statement.executeQuery().use(f)
    }
}

fun String.executeUpdate(connection: Connection, f: (PreparedStatement) -> Unit) {
    logger.debug { "EXECUTING SQL:\n$this" }

    connection.prepareStatement(this.trimIndent()).use(f)
}

fun String.executeUpdate(connection: Connection, vararg args: String) {
    this.executeUpdate(connection) { statement ->
        args.forEachIndexed { index, arg -> statement.setString(index + 1, arg) }
        statement.executeUpdate()
    }
}

fun String.toQuery(vararg args: String): String = this.trimIndent().replace("?", "%s").format(*args)

fun String.toQuery(context: Map<String, String>): String =
    this.trimIndent().replace(VAR_REGEX) {
        context[it.groupValues[1]] ?: throw IllegalStateException("Context is missing ${it.value}")
    }

fun shortHash(hashCode: Int): String = "%02x".format(hashCode)

private val VAR_REGEX = "\\?(\\w+)".toRegex()

private const val SCHEMA_KEY = "schema"
private const val TABLE_KEY = "table"
private const val COLUMNS_KEY = "columns"
private const val TEMPLATE_COLUMNS_KEY = "templateColumns"
private const val UNIQUENESS_CONSTRAINT_KEY = "uniquenessConstraint"
private const val UPDATE_STATEMENT_KEY = "updateStatement"
private const val INDEX_KEY = "index"
private const val SECONDARY_INDEX_KEY = "secondaryIndex"

const val GET_EXISTING_SCHEMA_QUERY =
    """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION ASC
    """

const val CREATE_SCHEMA_QUERY =
    """
        DECLARE @Schema VARCHAR(MAX) = ?
        IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = @Schema)
        BEGIN
            EXEC ('CREATE SCHEMA [' + @Schema + ']');
        END
    """

const val CREATE_TABLE_QUERY =
    """
        IF OBJECT_ID('?$SCHEMA_KEY.?$TABLE_KEY') IS NULL
        BEGIN
            CREATE TABLE [?$SCHEMA_KEY].[?$TABLE_KEY]
            (
                ?$COLUMNS_KEY
            );
            ?$INDEX_KEY;
            ?$SECONDARY_INDEX_KEY;
        END
    """

const val CREATE_INDEX_QUERY = """
        CREATE ? INDEX ? ON [?].[?] (?)
    """

const val DROP_TABLE_QUERY = """
        DROP TABLE [?].[?];
    """

const val INSERT_INTO_QUERY =
    """
        SET NOCOUNT ON;
        INSERT INTO [?$SCHEMA_KEY].[?$TABLE_KEY] WITH (TABLOCK) (?$COLUMNS_KEY)
            SELECT table_value.*
            FROM (VALUES (?$TEMPLATE_COLUMNS_KEY)) table_value(?$COLUMNS_KEY)
    """

const val MERGE_INTO_QUERY =
    """
        SET NOCOUNT ON;
        MERGE INTO [?$SCHEMA_KEY].[?$TABLE_KEY] WITH (TABLOCK) AS Target
        USING (VALUES (?$TEMPLATE_COLUMNS_KEY)) AS Source (?$COLUMNS_KEY)
        ON ?$UNIQUENESS_CONSTRAINT_KEY
        WHEN MATCHED THEN
            UPDATE SET ?$UPDATE_STATEMENT_KEY
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (?$COLUMNS_KEY) VALUES (?$COLUMNS_KEY)
        ;
    """

// Insert-only variant used for user-managed (foreign) tables: new rows are
// inserted, rows that already exist on the target are left untouched.
const val MERGE_INSERT_ONLY_QUERY =
    """
        SET NOCOUNT ON;
        MERGE INTO [?$SCHEMA_KEY].[?$TABLE_KEY] WITH (TABLOCK) AS Target
        USING (VALUES (?$TEMPLATE_COLUMNS_KEY)) AS Source (?$COLUMNS_KEY)
        ON ?$UNIQUENESS_CONSTRAINT_KEY
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (?$COLUMNS_KEY) VALUES (?$COLUMNS_KEY)
        ;
    """

const val ALTER_TABLE_ADD = """
        ALTER TABLE [?].[?]
        ADD [?] ? NULL;
    """

const val ALTER_TABLE_DROP = """
        ALTER TABLE [?].[?]
        DROP COLUMN [?];
    """
const val ALTER_TABLE_MODIFY =
    """
        ALTER TABLE [?].[?]
        ALTER COLUMN [?] ? NULL;
    """

const val DELETE_WHERE_COL_IS_NOT_NULL =
    """
        SET NOCOUNT ON;
        DELETE FROM [?].[?] WITH (TABLOCK)
        WHERE [?] is not NULL
    """

const val DELETE_WHERE_COL_LESS_THAN =
    """
        SET NOCOUNT ON;
        DELETE FROM [?].[?] WITH (TABLOCK)
        WHERE [?] < ?
    """

const val HAS_IDENTITY_COLUMN_QUERY =
    """
        SELECT COUNT(*)
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND c.is_identity = 1
    """

const val SELECT_FROM = """
        SELECT *
        FROM [?].[?]
    """

const val COUNT_FROM = """
        SELECT COUNT(*)
        FROM [?].[?]
    """

class MSSQLQueryBuilder(
    defaultSchema: String,
    private val stream: DestinationStream,
) {
    companion object {

        const val SQL_ERROR_OBJECT_EXISTS = 2714
        const val AIRBYTE_CDC_DELETED_AT = "_ab_cdc_deleted_at"
        const val DEFAULT_SEPARATOR = ",\n        "

        val airbyteFinalTableFields =
            listOf(
                NamedField(COLUMN_NAME_AB_RAW_ID, FieldType(StringType, false)),
                NamedField(COLUMN_NAME_AB_EXTRACTED_AT, FieldType(IntegerType, false)),
                NamedField(COLUMN_NAME_AB_META, FieldType(ObjectTypeWithoutSchema, false)),
                NamedField(COLUMN_NAME_AB_GENERATION_ID, FieldType(IntegerType, false)),
            )

        val airbyteFields = airbyteFinalTableFields.map { it.name }.toSet()
    }

    data class NamedField(val name: String, val type: FieldType)
    data class NamedValue(val name: String, val value: AirbyteValue)
    data class NamedSqlField(val name: String, val type: MssqlType)

    val outputSchema: String = stream.mappedDescriptor.namespace ?: defaultSchema
    val tableName: String = stream.mappedDescriptor.name
    val uniquenessKey: List<String> =
        when (stream.importType) {
            is Dedupe ->
                if ((stream.importType as Dedupe).primaryKey.isNotEmpty()) {
                    (stream.importType as Dedupe).primaryKey.map { it.joinToString(".") }
                } else {
                    listOf((stream.importType as Dedupe).cursor.joinToString("."))
                }
            Append -> emptyList()
            Overwrite -> emptyList()
            SoftDelete,
            Update -> throw ConfigErrorException("Unsupported sync mode: ${stream.importType}")
        }

    private val toMssqlType = AirbyteTypeToMssqlType

    val finalTableSchema: List<NamedField> = airbyteFinalTableFields + extractFinalTableSchema()
    val hasCdc: Boolean = finalTableSchema.any { it.name == AIRBYTE_CDC_DELETED_AT }

    private val indexedColumns: Set<String> =
        if (hasCdc) uniquenessKey.toSet() + AIRBYTE_CDC_DELETED_AT else uniquenessKey.toSet()

    /**
     * A table that already exists but lacks Airbyte's internal metadata columns is treated as
     * user-managed ("foreign"): no DDL is ever issued against it, writes use stream columns
     * only, and dedupe merges insert new keys without updating existing rows.
     */
    @Volatile var isForeignTable: Boolean = false
        private set

    private val dataTableSchema: List<NamedField> by lazy { extractFinalTableSchema() }

    /** Schema used for INSERT/MERGE statements: stream columns only for foreign tables. */
    val insertTableSchema: List<NamedField>
        get() = if (isForeignTable) dataTableSchema else finalTableSchema

    private data class ExistingColumn(val name: String, val typeName: String)

    private fun getExistingColumns(connection: Connection): List<ExistingColumn> {
        val columns = mutableListOf<ExistingColumn>()
        GET_EXISTING_SCHEMA_QUERY.executeQuery(connection, outputSchema, tableName) { rs ->
            while (rs.next()) {
                columns.add(
                    ExistingColumn(
                        rs.getString("COLUMN_NAME"),
                        rs.getString("DATA_TYPE").uppercase()
                    )
                )
            }
        }
        return columns
    }

    private fun getExistingSchema(
        connection: Connection,
        existingColumns: List<ExistingColumn>
    ): List<NamedSqlField> =
        existingColumns.mapNotNull { column ->
            runCatching { MssqlType.valueOf(column.typeName) }.getOrElse {
                logger.warn {
                    "Column ${column.name} in [$outputSchema].[$tableName] has unsupported" +
                        " type ${column.typeName}; skipping it in schema comparison"
                }
                null
            }?.let { NamedSqlField(column.name, it) }
        }

    private fun getSchema(): List<NamedSqlField> =
        finalTableSchema.map { NamedSqlField(it.name, toMssqlType.convert(it.type.type)) }

    fun updateSchema(connection: Connection) {
        val existingColumns = getExistingColumns(connection)

        // A pre-existing table without Airbyte's internal columns is user-managed.
        // Never run DDL against it; writes will use stream columns only.
        if (existingColumns.isNotEmpty() && existingColumns.none { it.name in airbyteFields }) {
            isForeignTable = true
            logger.info {
                "Table [$outputSchema].[$tableName] is user-managed;" +
                    " skipping schema evolution and writing stream columns only"
            }
            return
        }

        val existingSchema = getExistingSchema(connection, existingColumns)

        val expectedSchema = getSchema()

        val existingFields = existingSchema.associate { it.name to it.type }
        val expectedFields = expectedSchema.associate { it.name to it.type }

        if (existingFields == expectedFields) {
            return
        }

        val toDelete = existingFields.filter { it.key !in expectedFields }
        val toAdd = expectedFields.filter { it.key !in existingFields }
        val toAlter =
            expectedFields.filter { it.key in existingFields && it.value != existingFields[it.key] }

        val query =
            StringBuilder()
                .apply {
                    toDelete.entries.forEach {
                        appendLine(ALTER_TABLE_DROP.toQuery(outputSchema, tableName, it.key))
                    }
                    toAdd.entries.forEach {
                        appendLine(
                            ALTER_TABLE_ADD.toQuery(
                                outputSchema,
                                tableName,
                                it.key,
                                it.value.sqlString
                            )
                        )
                    }
                    toAlter.entries.forEach {
                        appendLine(
                            ALTER_TABLE_MODIFY.toQuery(
                                outputSchema,
                                tableName,
                                it.key,
                                it.value.sqlString
                            )
                        )
                    }
                }
                .toString()

        query.executeUpdate(connection)
    }

    fun createTableIfNotExists(connection: Connection) {
        try {
            CREATE_SCHEMA_QUERY.executeUpdate(connection, outputSchema)
        } catch (e: SQLServerException) {
            // MSSQL create schema if not exists isn't atomic. Ignoring this error when it happens.
            if (e.sqlServerError.errorNumber != SQL_ERROR_OBJECT_EXISTS) {
                throw e
            }
        }

        createTableIfNotExistsQuery(finalTableSchema).executeUpdate(connection)
    }

    fun dropTable(connection: Connection) {
        if (isForeignTable) {
            logger.warn { "Refusing to drop user-managed table [$outputSchema].[$tableName]" }
            return
        }
        DROP_TABLE_QUERY.toQuery(outputSchema, tableName).executeUpdate(connection)
    }

    fun getFinalTableInsertColumnHeader(): String =
        if (isForeignTable) {
            getForeignTableInsertColumnHeader(dataTableSchema)
        } else {
            getFinalTableInsertColumnHeader(finalTableSchema)
        }

    private fun getForeignTableInsertColumnHeader(schema: List<NamedField>): String {
        val columns = schema.joinToString(", ") { "[${it.name}]" }
        val templateColumns = schema.joinToString(", ") { "?" }
        if (uniquenessKey.isEmpty()) {
            return INSERT_INTO_QUERY.toQuery(
                mapOf(
                    SCHEMA_KEY to outputSchema,
                    TABLE_KEY to tableName,
                    COLUMNS_KEY to columns,
                    TEMPLATE_COLUMNS_KEY to templateColumns,
                )
            )
        }
        // Insert new keys only; never UPDATE rows that already exist on a user-managed table.
        val uniquenessConstraint =
            uniquenessKey.joinToString(" AND ") { "Target.[$it] = Source.[$it]" }
        return MERGE_INSERT_ONLY_QUERY.toQuery(
            mapOf(
                SCHEMA_KEY to outputSchema,
                TABLE_KEY to tableName,
                TEMPLATE_COLUMNS_KEY to templateColumns,
                COLUMNS_KEY to columns,
                UNIQUENESS_CONSTRAINT_KEY to uniquenessConstraint,
            )
        )
    }

    /**
     * True when a user-managed table has an IDENTITY column. Explicit identity values then
     * require SET IDENTITY_INSERT ON for the session (a session flag, not DDL).
     */
    fun hasIdentityColumn(connection: Connection): Boolean =
        HAS_IDENTITY_COLUMN_QUERY.executeQuery(connection, outputSchema, tableName) { rs ->
            rs.next() && rs.getInt(1) > 0
        }

    fun setIdentityInsert(connection: Connection, enabled: Boolean) {
        // Must go through a raw Statement (TDS batch). PreparedStatement executes as an
        // sp_executesql RPC whose SET options revert when the nested batch returns.
        val sql =
            "SET IDENTITY_INSERT [$outputSchema].[$tableName] ${if (enabled) "ON" else "OFF"}"
        connection.createStatement().use { it.execute(sql) }
        logger.info { "IDENTITY_INSERT [$outputSchema].[$tableName] set to ${if (enabled) "ON" else "OFF"}" }
    }

    fun deleteCdc(connection: Connection) {
        if (isForeignTable) {
            return
        }
        DELETE_WHERE_COL_IS_NOT_NULL.toQuery(outputSchema, tableName, AIRBYTE_CDC_DELETED_AT)
            .executeUpdate(connection)
    }

    fun deletePreviousGenerations(connection: Connection, minGenerationId: Long) {
        if (isForeignTable) {
            return
        }
        DELETE_WHERE_COL_LESS_THAN.toQuery(
                outputSchema,
                tableName,
                COLUMN_NAME_AB_GENERATION_ID,
                minGenerationId.toString(),
            )
            .executeUpdate(connection)
    }

    /**
     * Binds the field's raw JSON text for a user-managed table. SQL Server implicitly converts
     * nvarchar literals to the target column type (datetime2, bigint, float, bit,
     * uniqueidentifier, ...). Used when the enriched/coerced value is missing or was
     * nullified upstream, so real values are never silently dropped on foreign tables.
     */
    private fun bindRawValue(
        statement: PreparedStatement,
        statementIndex: Int,
        field: NamedField,
        rawJson: com.fasterxml.jackson.databind.JsonNode
    ): Boolean {
        val node = rawJson.get(field.name) ?: return false
        if (node.isNull) return false
        statement.setString(
            statementIndex,
            if (node.isValueNode) node.asText() else node.toString()
        )
        return true
    }

    fun populateStatement(
        statement: PreparedStatement,
        plainRecord: DestinationRecordRaw,
        schema: List<NamedField>
    ) {
        val enrichedRecord = plainRecord.asEnrichedDestinationRecordAirbyteValue()
        val populatedFields = enrichedRecord.allTypedFields
        val rawJson by lazy(LazyThreadSafetyMode.NONE) { plainRecord.asJsonRecord() }

        var airbyteMetaStatementIndex: Int? = null
        schema.forEachIndexed { index, field ->
            val statementIndex = index + 1
            val value = populatedFields[field.name]
            if (value == null || value.abValue == NullValue) {
                if (!isForeignTable || !bindRawValue(statement, statementIndex, field, rawJson)) {
                    statement.setAsNullValue(statementIndex, field.type.type)
                }
                return@forEachIndexed
            }
            if (value.airbyteMetaField == Meta.AirbyteMetaFields.META) {
                // don't populate _airbyte_meta yet - we might run into errors in the other fields
                // for this record.
                // Instead, we grab the statement index, and populate airbyte_meta after processing
                // all the other fields.
                airbyteMetaStatementIndex = statementIndex
                return@forEachIndexed
            }

            // Apply shared MSSQL coercion: range validation + complex-type serialisation.
            // Foreign tables skip range nullification — the real column types decide validity.
            if (isForeignTable) {
                MSSQLValueCoercer.coerceForForeignTable(value)
            } else {
                MSSQLValueCoercer.coerce(value)
            }
            if (value.abValue is NullValue) {
                if (!isForeignTable || !bindRawValue(statement, statementIndex, field, rawJson)) {
                    statement.setAsNullValue(statementIndex, field.type.type)
                }
                return@forEachIndexed
            }

            // INSERT-specific: set typed JDBC parameters
            when (value.type) {
                BooleanType ->
                    statement.setBoolean(statementIndex, (value.abValue as BooleanValue).value)
                DateType ->
                    statement.setDate(
                        statementIndex,
                        Date.valueOf((value.abValue as DateValue).value)
                    )
                IntegerType ->
                    statement.setLong(
                        statementIndex,
                        (value.abValue as IntegerValue).value.longValueExact()
                    )
                NumberType ->
                    statement.setBigDecimal(statementIndex, (value.abValue as NumberValue).value)
                StringType ->
                    statement.setString(statementIndex, (value.abValue as StringValue).value)
                TimeTypeWithTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimeWithTimezoneValue).value
                    )
                TimeTypeWithoutTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimeWithoutTimezoneValue).value
                    )
                TimestampTypeWithTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimestampWithTimezoneValue).value
                    )
                TimestampTypeWithoutTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimestampWithoutTimezoneValue).value
                    )

                // Complex types already serialised to StringValue by MSSQLValueCoercer.coerce()
                is ArrayType,
                ArrayTypeWithoutSchema,
                is ObjectType,
                ObjectTypeWithEmptySchema,
                ObjectTypeWithoutSchema,
                is UnionType,
                is UnknownType ->
                    statement.setString(statementIndex, (value.abValue as StringValue).value)
            }
        }

        // Now that we're done processing the rest of the record, populate airbyte_meta into the
        // prepared statement.
        airbyteMetaStatementIndex?.let { statementIndex ->
            statement.setString(
                statementIndex,
                enrichedRecord.airbyteMeta.abValue.serializeToString()
            )
        }
    }

    fun readResult(rs: ResultSet, schema: List<NamedField>): ObjectValue {
        val valueMap =
            schema
                .filter { field -> field.name !in airbyteFields }
                .map { field -> rs.getAirbyteNamedValue(field) }
                .associate { it.name to it.value }
        return ObjectValue.from(valueMap)
    }

    private fun createTableIfNotExistsQuery(schema: List<NamedField>): String {
        val fqTableName = "$outputSchema.$tableName"
        val index =
            if (uniquenessKey.isNotEmpty())
                createIndex(fqTableName, uniquenessKey, clustered = false)
            else ""
        val cdcIndex = if (hasCdc) createIndex(fqTableName, listOf(AIRBYTE_CDC_DELETED_AT)) else ""

        return CREATE_TABLE_QUERY.toQuery(
            mapOf(
                SCHEMA_KEY to outputSchema,
                TABLE_KEY to tableName,
                COLUMNS_KEY to airbyteTypeToSqlSchema(schema),
                INDEX_KEY to index,
                SECONDARY_INDEX_KEY to cdcIndex,
            )
        )
    }

    private fun createIndex(
        fqTableName: String,
        columns: List<String>,
        clustered: Boolean = false
    ): String {
        val name = "[${fqTableName.replace('.', '_')}_${shortHash(columns.hashCode())}]"
        val indexType = if (clustered) "CLUSTERED" else ""
        return CREATE_INDEX_QUERY.toQuery(
            indexType,
            name,
            outputSchema,
            tableName,
            columns.joinToString(", ") { "[$it]" }
        )
    }

    private fun getFinalTableInsertColumnHeader(schema: List<NamedField>): String {
        val columns = schema.joinToString(", ") { "[${it.name}]" }
        val templateColumns = schema.joinToString(", ") { "?" }
        return if (uniquenessKey.isEmpty()) {
            INSERT_INTO_QUERY.toQuery(
                mapOf(
                    SCHEMA_KEY to outputSchema,
                    TABLE_KEY to tableName,
                    COLUMNS_KEY to columns,
                    TEMPLATE_COLUMNS_KEY to templateColumns,
                )
            )
        } else {
            val uniquenessConstraint =
                uniquenessKey.joinToString(" AND ") { "Target.[$it] = Source.[$it]" }
            val updateStatement =
                schema.joinToString(", ") { "Target.[${it.name}] = Source.[${it.name}]" }
            MERGE_INTO_QUERY.toQuery(
                mapOf(
                    SCHEMA_KEY to outputSchema,
                    TABLE_KEY to tableName,
                    TEMPLATE_COLUMNS_KEY to templateColumns,
                    COLUMNS_KEY to columns,
                    UNIQUENESS_CONSTRAINT_KEY to uniquenessConstraint,
                    UPDATE_STATEMENT_KEY to updateStatement,
                )
            )
        }
    }

    private fun extractFinalTableSchema(): List<NamedField> =
        stream.schema.asColumns()
            .filter { it.key !in airbyteFields }
            .map { NamedField(name = it.key, type = it.value) }
            .toList()

    private fun airbyteTypeToSqlSchema(
        schema: List<NamedField>,
        separator: String = DEFAULT_SEPARATOR
    ): String {
        return schema.joinToString(separator = separator) {
            val mssqlType =
                toMssqlType.convert(
                    it.type.type,
                    isIndexed = indexedColumns.contains(it.name),
                )
            "[${it.name}] ${mssqlType.sqlString} NULL"
        }
    }
}
