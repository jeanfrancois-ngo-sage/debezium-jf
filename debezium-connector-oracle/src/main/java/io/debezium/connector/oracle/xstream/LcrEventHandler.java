/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource.PositionAndScn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import oracle.streams.ChunkColumnValue;
import oracle.streams.DDLLCR;
import oracle.streams.DefaultRowLCR;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamLCRCallbackHandler;
import oracle.streams.XStreamOut;

/**
 * Handler for Oracle DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * @author Gunnar Morling
 */
class LcrEventHandler implements XStreamLCRCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LcrEventHandler.class);

    private final OracleConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final XstreamStreamingChangeEventSource eventSource;
    private final XStreamStreamingChangeEventSourceMetrics streamingMetrics;
    private final Map<String, ChunkColumnValues> columnChunks;
    private RowLCR currentRow;
    private final Map<String, Long> processLcrInvocationCountPerTable = new HashMap<>(); // Track how many times processLCR is called per table
    private volatile boolean lcrWasProcessedInLastCallback = false; // Track if LCR was actually received
    private static final int MAX_TABLE_TRACKING_SIZE = 1000; // Maximum number of tables to track invocation count

    LcrEventHandler(OracleConnectorConfig connectorConfig, ErrorHandler errorHandler,
                    EventDispatcher<OraclePartition, TableId> dispatcher, Clock clock,
                    OracleDatabaseSchema schema, OraclePartition partition, OracleOffsetContext offsetContext,
                    boolean tablenameCaseInsensitive, XstreamStreamingChangeEventSource eventSource,
                    XStreamStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
        this.eventSource = eventSource;
        this.streamingMetrics = streamingMetrics;
        this.columnChunks = new LinkedHashMap<>();
    }

    @Override
    public void processLCR(LCR lcr) throws StreamsException {
        lcrWasProcessedInLastCallback = true;

        long start = System.nanoTime();
        long startMs = System.currentTimeMillis();
        String randomUUIDString = UUID.randomUUID().toString();

        TableId tableId = getTableId(lcr);

        String tableKey = tableId.toString();
        long tableInvocationCount;
        if (processLcrInvocationCountPerTable.size() < MAX_TABLE_TRACKING_SIZE || processLcrInvocationCountPerTable.containsKey(tableKey)) {
            tableInvocationCount = processLcrInvocationCountPerTable.compute(tableKey, (k, v) -> (v == null) ? 1L : v + 1L);
        } else {
            tableInvocationCount = 0; // Don't track new tables if we've hit the limit
            LOGGER.warn("Table tracking limit ({}) reached, not tracking invocation count for table: {}", MAX_TABLE_TRACKING_SIZE, tableId);
        }

        boolean isFiltered = !connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId);

        if (isFiltered) {
            columnChunks.clear();
            currentRow = null;
            LOGGER.info("Skipping LCR for excluded table: {} (table invocations: {})", tableId, tableInvocationCount);
            return;
        }

        LOGGER.trace("Received LCR {}", lcr);
        LOGGER.trace("Processing LCR from SCN {}", offsetContext.getScn());
        try {
            LOGGER.info("[{} LcrEventHandler] processLCR invocation #{} - LCR type: {}, table: {}", randomUUIDString, tableInvocationCount, lcr.getCommandType(), tableId);

            setWatermark();
            columnChunks.clear();

            final LcrPosition lcrPosition = new LcrPosition(lcr.getPosition());

            // After a restart it may happen we get the event with the last processed LCR again
            LcrPosition offsetLcrPosition = LcrPosition.valueOf(offsetContext.getLcrPosition());
            if (lcrPosition.compareTo(offsetLcrPosition) <= 0) {
                final LcrPosition recPosition = offsetLcrPosition;
                LOGGER.info("{} Ignoring change event with already processed SCN/LCR Position {}/{}, last recorded {}/{}",
                        randomUUIDString, lcrPosition,
                        lcrPosition.getScn(),
                        recPosition != null ? recPosition : "none",
                        recPosition != null ? recPosition.getScn() : "none");
                return;
            }

            offsetContext.setRowId(""); // specifically reset on each event
            offsetContext.setScn(lcrPosition.getScn());
            offsetContext.setEventCommitScn(lcrPosition.getCommitScn());
            offsetContext.setEventScn(lcrPosition.getScn());
            offsetContext.setLcrPosition(lcrPosition.toString());
            offsetContext.setTransactionId(lcr.getTransactionId());
            offsetContext.tableEvent(new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName()),
                    lcr.getSourceTime().timestampValue().toInstant());

            long processingStart = System.nanoTime();
            if (lcr instanceof RowLCR) {
                LOGGER.info("[{} LcrEventHandler] Processing RowLCR - command: {}", randomUUIDString, ((RowLCR) lcr).getCommandType());
                processRowLCR((RowLCR) lcr);
            }
            else if (lcr instanceof DDLLCR) {
                LOGGER.info("[{} LcrEventHandler] Processing DDL LCR", randomUUIDString);
                dispatchSchemaChangeEvent((DDLLCR) lcr);
            }
            long processingDuration = (System.nanoTime() - processingStart) / 1_000_000;
            LOGGER.info("[{} LcrEventHandler-Perf] LCR processing (dispatch) took {} ms", randomUUIDString, processingDuration);
        }
        // nothing to be done here if interrupted; the event loop will be stopped in the streaming source
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("{} Received signal to stop, event loop will halt", randomUUIDString);
        }
        // XStream's receiveLCRCallback() doesn't reliably propagate exceptions, so we do that ourselves here
        catch (Exception e) {
            LOGGER.info("Error during processLCR: {}", e.getMessage());
            errorHandler.setProducerThrowable(e);
        } finally {
            long end = System.nanoTime();
            long endMs = System.currentTimeMillis();
            long durationNs = end - start;
            long durationMs = endMs - startMs;

            LOGGER.info("[{} LcrEventHandler] *** LCR COMPLETED *** - total time: {} ms ({} ns), SCN: {}, table: {}.{}",
                    randomUUIDString, durationMs, durationNs, offsetContext.getScn(),
                    lcr.getObjectOwner(), lcr.getObjectName());
        }
    }

    private void processRowLCR(RowLCR row) throws InterruptedException {
        if (row.getCommandType().equals(RowLCR.LOB_ERASE)) {
            LOGGER.warn("LOB_ERASE for table '{}' is not supported, "
                    + "use DML operations to manipulate LOB columns only.", row.getObjectName());
            return;
        }

        if (row.hasChunkData()) {
            // If the row has chunk data, the RowLCR cannot be immediately dispatched.
            // The handler needs to cache the current row and wait for the chunks to be delivered before
            // the event can be safely dispatched. See processChunk below.
            currentRow = row;
        }
        else {
            // Since the row has no chunk data, it can be dispatched immediately.
            dispatchDataChangeEvent(row, null);
        }
    }

    private void dispatchDataChangeEvent(RowLCR lcr, Map<String, Object> chunkValues) throws InterruptedException {
        LOGGER.debug("Processing DML event {}", lcr);

        long dispatchStart = System.currentTimeMillis();
        String randomUUIDString = UUID.randomUUID().toString();
        LOGGER.info("[{} LcrEventHandler] dispatchDataChangeEvent START - table: {}.{}, command: {}",
                randomUUIDString, lcr.getObjectOwner(), lcr.getObjectName(), lcr.getCommandType());
        if (RowLCR.COMMIT.equals(lcr.getCommandType())) {
            final Instant commitTimestamp = lcr.getSourceTime().timestampValue().toInstant();
            LOGGER.info("[{} LcrEventHandler] Dispatching COMMIT event - timestamp: {}", randomUUIDString, commitTimestamp);
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, commitTimestamp);
            return;
        }

        TableId tableId = getTableId(lcr);

        LOGGER.info("[{} LcrEventHandler] Processing table: {}", randomUUIDString, tableId);

        Table table = schema.tableFor(tableId);
        if (table == null) {
            if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Table {} is new but excluded, schema change skipped.", tableId);
                return;
            }

            LOGGER.warn("Obtaining schema for table {}, which should be already loaded, this may signal potential bug in fetching table schemas.", tableId);
            final String tableDdl;
            try {
                tableDdl = getTableMetadataDdl(tableId);
            }
            catch (NonRelationalTableException e) {
                LOGGER.warn("{} The event will be skipped.", e.getMessage());
                streamingMetrics.incrementWarningCount();
                return;
            }

            LOGGER.info("Table {} will be captured.", tableId);
            dispatcher.dispatchSchemaChangeEvent(
                    partition,
                    offsetContext,
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            tableDdl,
                            schema,
                            Instant.now(),
                            streamingMetrics,
                            null));

            table = schema.tableFor(tableId);
            if (table == null) {
                return;
            }
        }

        // Xstream does not provide any before state for LOB columns and so this map will be
        // populated here by column name with the OracleValueConverters.UNAVAILABLE_VALUE.
        Map<String, Object> oldChunkValues = new HashMap<>(0);

        if (chunkValues == null) {
            // Happens when dispatching an LCR without any chunk data.
            chunkValues = new HashMap<>(0);
        }

        // LCR events may arrive both with and without chunk data.
        //
        // For example a DELETE by a primary key on a table with LOB columns will not supply any
        // LOB chunk data. In other scenarios such as an UPDATE where a LOB column is modified,
        // the updated LOB value will be provided but the prior value will not be.
        //
        // So in either case, the values need to be serialized here such that any LOB column that
        // is not explicitly provided in the map is initialized with the unavailable value
        // marker object so its transformed correctly by the value converters.

        for (Column column : schema.getLobColumnsForTable(table.id())) {
            // again Xstream doesn't supply before state for LOB values; explicitly use unavailable value
            oldChunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            if (!chunkValues.containsKey(column.name())) {
                // Column not supplied, initialize with unavailable value marker
                LOGGER.trace("\tColumn '{}' not supplied, initialized with unavailable value", column.name());
                chunkValues.put(column.name(), OracleValueConverters.UNAVAILABLE_VALUE);
            }
        }

        final Object rowIdObject = lcr.getAttribute("ROW_ID");
        if (rowIdObject != null) {
            offsetContext.setRowId(rowIdObject.toString());
        }

        dispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new XStreamChangeRecordEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        lcr,
                        oldChunkValues,
                        chunkValues,
                        schema.tableFor(tableId),
                        schema,
                        clock));

        long dispatchEnd = System.currentTimeMillis();
        LOGGER.info("[{} LcrEventHandler] dispatchDataChangeEvent COMPLETED in {} ms", randomUUIDString, dispatchEnd - dispatchStart);
    }

    private void dispatchSchemaChangeEvent(DDLLCR ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLText());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        ddlLcr.getSourceDatabaseName(),
                        ddlLcr.getObjectOwner(),
                        ddlLcr.getDDLText(),
                        schema,
                        ddlLcr.getSourceTime().timestampValue().toInstant(),
                        streamingMetrics,
                        () -> processTruncateEvent(ddlLcr)));
    }

    private void processTruncateEvent(DDLLCR ddlLcr) {
        LOGGER.debug("Handling truncate event");
        DefaultRowLCR rowLCR = new DefaultRowLCR(
                ddlLcr.getSourceDatabaseName(),
                ddlLcr.getCommandType(),
                ddlLcr.getObjectOwner(),
                ddlLcr.getObjectName(),
                ddlLcr.getTransactionId(),
                ddlLcr.getTag(),
                ddlLcr.getPosition(),
                ddlLcr.getSourceTime());
        try {
            dispatchDataChangeEvent(rowLCR, null);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }

    }

    private TableId getTableId(LCR lcr) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
        }
        else {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName().toLowerCase());
        }
    }

    private String getTableMetadataDdl(TableId tableId) throws NonRelationalTableException {
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        final String pdbName = connectorConfig.getPdbName();
        // A separate connection must be used for this out-of-bands query while processing the Xstream callback.
        // This should have negligible overhead as this should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), false)) {
            if (!Strings.isNullOrBlank(pdbName)) {
                connection.setSessionToPdb(pdbName);
            }
            connection.setAutoCommit(false);
            return connection.getTableMetadataDdl(tableId);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to get table DDL metadata for: " + tableId, e);
        }
    }

    private void setWatermark() {
        if (eventSource.getXsOut() == null) {
            LOGGER.debug("Skipping watermark update - XStream connection is null");
            return;
        }
        try {
            final PositionAndScn message = eventSource.receivePublishedPosition();
            if (message == null) {
                LOGGER.trace("No pending watermark update");
                return;
            }

            long currentTimeMillis = System.currentTimeMillis();
            LOGGER.info("WATERMARK UPDATE: Applying committed offset to Oracle XStream at {}",
                       java.time.Instant.ofEpochMilli(currentTimeMillis));

            if (message.position != null) {
                // Log the SCN/timestamp we're about to commit
                LOGGER.info("WATERMARK: Setting processed low watermark with LCR position: {} (SCN from position: {})",
                           message.position, message.position.getScn());

                long startTime = System.currentTimeMillis();
                eventSource.getXsOut().setProcessedLowWatermark(
                        message.position.getRawPosition(),
                        XStreamOut.DEFAULT_MODE);
                long duration = System.currentTimeMillis() - startTime;

                // Calculate how old this event is
                java.time.Instant positionTimestamp = message.position.getTimestamp();
                if (positionTimestamp != null) {
                    long ageSeconds = (currentTimeMillis - positionTimestamp.toEpochMilli()) / 1000;
                    if (ageSeconds > 180) {
                        LOGGER.warn("WATERMARK LAG WARNING: Set watermark for event from {} - that's {} seconds ({} min) old! This will update LAST_SENT_MESSAGE_CREATE_TIME to an old timestamp.",
                                   positionTimestamp, ageSeconds, ageSeconds / 60);
                    } else {
                        LOGGER.info("WATERMARK: Successfully set position watermark (took {}ms) - Event age: {} seconds - V$XSTREAM_OUTBOUND_SERVER.LAST_SENT_MESSAGE_CREATE_TIME will be set to: {}",
                                   duration, ageSeconds, positionTimestamp);
                    }
                } else {
                    LOGGER.info("WATERMARK: Successfully set position watermark (took {}ms) - V$XSTREAM_OUTBOUND_SERVER.LAST_SENT_MESSAGE_CREATE_TIME should now be updated",
                               duration);
                }
            }
            else if (message.scn != null) {
                LOGGER.info("WATERMARK: Setting processed low watermark with SCN bytes");
                long startTime = System.currentTimeMillis();
                eventSource.getXsOut().setProcessedLowWatermark(
                        message.scn,
                        XStreamOut.DEFAULT_MODE);
                long duration = System.currentTimeMillis() - startTime;
                LOGGER.info("WATERMARK: Successfully set SCN watermark (took {}ms) - V$XSTREAM_OUTBOUND_SERVER.LAST_SENT_MESSAGE_CREATE_TIME should now be updated",
                           duration);
            }
            else {
                LOGGER.warn("WATERMARK: Cannot update - both position and SCN are null in offset message");
                return;
            }
        }
        catch (StreamsException e) {
            LOGGER.error("CRITICAL: Failed to set processed low watermark in Oracle XStream", e);
            LOGGER.error("Oracle StreamsException details - Error code: {}, SQL state: {}, Message: {}",
                        e.getErrorCode(), e.getSQLState(), e.getMessage());
            throw new DebeziumException("Couldn't set processed low watermark", e);
        }
    }

    @Override
    public void processChunk(ChunkColumnValue chunk) throws StreamsException {
        // If currentRow is null, it means the LCR was filtered out (excluded table)
        // Skip processing chunks for excluded tables
        if (currentRow == null) {
            LOGGER.info("Skipping chunk for excluded table (currentRow is null)");
            return;
        }

        columnChunks.computeIfAbsent(chunk.getColumnName(), v -> new ChunkColumnValues()).add(chunk);
        if (chunk.isEndOfRow()) {
            resolveAndDispatchCurrentChunkedRow();
        }
    }

    @Override
    public LCR createLCR() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public ChunkColumnValue createChunk() throws StreamsException {
        throw new UnsupportedOperationException("Should never be called");
    }

    private void resolveAndDispatchCurrentChunkedRow() {
        try {
            // Map of resolved chunk values
            Map<String, Object> resolvedChunkValues = new HashMap<>();

            // All chunks have been dispatched to the event handler, combine the chunks now.
            for (Map.Entry<String, ChunkColumnValues> entry : columnChunks.entrySet()) {
                final String columnName = entry.getKey();
                final ChunkColumnValues chunkValues = entry.getValue();

                if (chunkValues.isEmpty()) {
                    LOGGER.trace("Column '{}' has no chunk values.", columnName);
                    continue;
                }

                final int type = chunkValues.getChunkType();
                switch (type) {
                    case ChunkColumnValue.CLOB:
                    case ChunkColumnValue.NCLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getStringValue());
                        break;

                    case ChunkColumnValue.XMLTYPE:
                        resolvedChunkValues.put(columnName, chunkValues.getXmlValue());
                        break;

                    case ChunkColumnValue.RAW:
                    case ChunkColumnValue.BLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getByteArray());
                        break;

                    default:
                        LOGGER.trace("Received an unsupported chunk type '{}' for column '{}', ignored.", type, columnName);
                        break;
                }
            }

            columnChunks.clear();
            dispatchDataChangeEvent(currentRow, resolvedChunkValues);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to process chunk data", e);
        }
    }

    /**
     * Check if an LCR was processed in the last receiveLCRCallback invocation and reset the flag.
     * @return true if processLCR was called, false otherwise
     */
    boolean checkAndResetLcrProcessedFlag() {
        boolean wasProcessed = lcrWasProcessedInLastCallback;
        lcrWasProcessedInLastCallback = false;
        return wasProcessed;
    }
}
