/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.Sizeable;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A queue which serves as handover point between producer threads (e.g. MySQL's
 * binlog reader thread) and the Kafka Connect polling loop.
 * <p>
 * The queue is configurable in different aspects, e.g. its maximum size and the
 * time to sleep (block) between two subsequent poll calls. See the
 * {@link Builder} for the different options. The queue applies back-pressure
 * semantics, i.e. if it holds the maximum number of elements, subsequent calls
 * to {@link #enqueue(T)} will block until elements have been removed from
 * the queue.
 * <p>
 * If an exception occurs on the producer side, the producer should make that
 * exception known by calling {@link #producerException(RuntimeException)} before stopping its
 * operation. Upon the next call to {@link #poll()}, that exception will be
 * raised, causing Kafka Connect to stop the connector and mark it as
 * {@code FAILED}.
 *
 * @author Gunnar Morling
 *
 * @param <T>
 *            the type of events in this queue. Usually {@link Sizeable} is
 *            used, but in cases where additional metadata must be passed from
 *            producers to the consumer, a custom type extending source records
 *            may be used.
 */
@ThreadSafe
public class ChangeEventQueue<T extends Sizeable> implements ChangeEventQueueMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;

    private final Lock lock;
    private final Condition isFull;
    private final Condition isNotFull;

    private final QueueProvider<T> queue;
    private final Supplier<PreviousContext> loggingContextSupplier;
    private final Queue<Long> sizeInBytesQueue;
    private long currentQueueSizeInBytes = 0;

    // Sometimes it is necessary to update the record before it is delivered depending on the content
    // of the following record. In that cases the easiest solution is to provide a single cell buffer
    // that will allow the modification of it during the explicit flush.
    // Typical example is MySQL connector when sometimes it is impossible to detect when the record
    // in process is the last one. In this case the snapshot flags are set during the explicit flush.
    @SingleThreadAccess("producer thread")
    private boolean buffering;

    private final AtomicReference<T> bufferedEvent = new AtomicReference<>();

    private volatile RuntimeException producerException;

    private ChangeEventQueue(Duration pollInterval, int maxQueueSize, int maxBatchSize, Supplier<LoggingContext.PreviousContext> loggingContextSupplier,
                             long maxQueueSizeInBytes, boolean buffering, QueueProvider<T> queueProvider) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.maxQueueSize = maxQueueSize;

        this.lock = new ReentrantLock();
        this.isFull = lock.newCondition();
        this.isNotFull = lock.newCondition();

        this.loggingContextSupplier = loggingContextSupplier;
        if (maxQueueSizeInBytes > 0) {
            this.sizeInBytesQueue = new ArrayDeque<>(maxQueueSize);
        }
        else {
            this.sizeInBytesQueue = new ArrayDeque<>(0);
        }

        this.maxQueueSizeInBytes = maxQueueSizeInBytes;
        this.buffering = buffering;
        this.queue = queueProvider;
    }

    public static class Builder<T extends Sizeable> {

        private Duration pollInterval;
        private int maxQueueSize;
        private int maxBatchSize;
        private Supplier<LoggingContext.PreviousContext> loggingContextSupplier;
        private long maxQueueSizeInBytes;
        private boolean buffering;
        private QueueProvider<T> queueProvider;

        public Builder<T> pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder<T> maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public Builder<T> maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder<T> loggingContextSupplier(Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
            this.loggingContextSupplier = loggingContextSupplier;
            return this;
        }

        public Builder<T> maxQueueSizeInBytes(long maxQueueSizeInBytes) {
            this.maxQueueSizeInBytes = maxQueueSizeInBytes;
            return this;
        }

        /**
         * Sets a custom {@link QueueProvider} implementation to be used by the {@link ChangeEventQueue}.
         * <p>
         * If not set, a default provider will be used internally based on the max queue size.
         * This allows for custom queue behavior or instrumentation if needed.
         *
         * @param queueProvider the queue provider to use; may be {@code null}, in which case a default will be used
         * @return this builder instance for method chaining
         */
        public ChangeEventQueue.Builder<T> queueProvider(QueueProvider<T> queueProvider) {
            this.queueProvider = queueProvider;
            return this;
        }

        public Builder<T> buffering() {
            this.buffering = true;
            return this;
        }

        public ChangeEventQueue<T> build() {
            QueueProvider<T> effectiveQueueProvider = (queueProvider != null) ? queueProvider : new DefaultQueueProvider<>(maxQueueSize);
            return new ChangeEventQueue<>(pollInterval, maxQueueSize, maxBatchSize, loggingContextSupplier, maxQueueSizeInBytes, buffering, effectiveQueueProvider);
        }
    }

    /**
     * Enqueues a record so that it can be obtained via {@link #poll()}. This method
     * will block if the queue is full.
     *
     * @param record
     *            the record to be enqueued
     * @throws InterruptedException
     *             if this thread has been interrupted
     */
    public void enqueue(T record) throws InterruptedException {
        if (record == null) {
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        long startTime = System.currentTimeMillis();
        if (buffering) {
            record = bufferedEvent.getAndSet(record);
            if (record == null) {
                // Can happen only for the first coming event
                return;
            }
        }

        doEnqueue(record);
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("PERF: Enqueued record in {}ms", duration);
    }

    /**
     * Applies a function to the event and the buffer and adds it to the queue. Buffer is emptied.
     *
     * @param recordModifier
     * @throws InterruptedException
     */
    public void flushBuffer(Function<T, T> recordModifier) throws InterruptedException {
        assert buffering : "Unsupported for queues with disabled buffering";
        T record = bufferedEvent.getAndSet(null);
        if (record != null) {
            doEnqueue(recordModifier.apply(record));
        }
    }

    /**
     * Disable buffering for the queue
     */
    public void disableBuffering() {
        assert bufferedEvent.get() == null : "Buffer must be flushed";
        buffering = false;
    }

    /**
     * Enable buffering for the queue
     */
    public void enableBuffering() {
        buffering = true;
    }

    protected void doEnqueue(T record) throws InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Enqueuing source record '{}'", maybeRedactSensitiveData(record));
        }

        String randomUUIDString = UUID.randomUUID().toString();
        int waitCycles = 0;
        long enqueueStartTime = System.currentTimeMillis();
        try {
            this.lock.lock();
            LOGGER.info("[{} doEnqueue] Lock ACQUIRED - queue: {}/{}, bytes: {}/{}",
                    randomUUIDString, queue.size(), maxQueueSize, currentQueueSizeInBytes, maxQueueSizeInBytes);

            while (queue.size() >= maxQueueSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                waitCycles++;
                boolean shouldLog = (waitCycles == 1) || (waitCycles % 20 == 0);
                if (shouldLog) {
                    long blockedDuration = System.currentTimeMillis() - enqueueStartTime;

                    if (waitCycles == 1) {
                        LOGGER.info("[{}] Queue FULL - Producer blocking (queue: {}/{}, bytes: {}/{})", randomUUIDString, queue.size(), maxQueueSize, currentQueueSizeInBytes, maxQueueSizeInBytes);
                    } else if (waitCycles >= 20) {
                        LOGGER.info("[{}] !!! Producer blocked {} cycles ({}ms) - Consumer too slow! Queue: {}/{}", randomUUIDString, waitCycles, blockedDuration, queue.size(), maxQueueSize);
                    } else {
                        LOGGER.info("[{}] Producer still blocked - cycle: {}, duration: {}ms, queue: {}/{}", randomUUIDString, waitCycles, blockedDuration, queue.size(), maxQueueSize);
                    }
                }

                // Signal poll() that queue is full and needs draining (while holding lock)
                LOGGER.info("[{} doEnqueue] SIGNALING isFull.signalAll() - notifying consumer (cycle {})", randomUUIDString, waitCycles);
                this.isFull.signalAll();

                // Wait for poll() to drain queue and signal that space is available
                // Note: await() automatically releases the lock while waiting and re-acquires it when signaled
                LOGGER.info("[{} doEnqueue] BEFORE isNotFull.await() - will release lock and wait {} ms (cycle {})",
                        randomUUIDString, pollInterval.toMillis(), waitCycles);
                long awaitStart = System.currentTimeMillis();
                boolean wasSignaled = this.isNotFull.await(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                long awaitDuration = System.currentTimeMillis() - awaitStart;

                if (wasSignaled) {
                    LOGGER.info("[{} doEnqueue] AFTER isNotFull.await() - SIGNALED by consumer after {} ms (cycle {}), queue now: {}/{}",
                            randomUUIDString, awaitDuration, waitCycles, queue.size(), maxQueueSize);
                } else {
                    LOGGER.info("[{} doEnqueue] AFTER isNotFull.await() - TIMEOUT after {} ms (cycle {}), queue still: {}/{} - NO SIGNAL RECEIVED!",
                            randomUUIDString, awaitDuration, waitCycles, queue.size(), maxQueueSize);
                }
            }

            if (waitCycles > 0) {
                long totalWaitTime = System.currentTimeMillis() - enqueueStartTime;
                LOGGER.info("[{}] Producer UNBLOCKED - waited {} cycles ({}ms), queue now: {}/{}", randomUUIDString, waitCycles, totalWaitTime, queue.size(), maxQueueSize);
            }

            LOGGER.info("[{} doEnqueue] Adding record to queue (current size: {})", randomUUIDString, queue.size());
            queue.enqueue(record);
            // If we pass a positiveLong max.queue.size.in.bytes to enable handling queue size in bytes feature
            if (maxQueueSizeInBytes > 0) {
                long messageSize = record.objectSize();
                sizeInBytesQueue.add(messageSize);
                currentQueueSizeInBytes += messageSize;
            }

            // batch size or queue sizeInBytes threshold reached
            if (queue.size() >= maxBatchSize || (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes >= maxQueueSizeInBytes)) {
                LOGGER.info("[{}] Threshold reached, notifying poll() to drain queue... (queue: {}/{})", randomUUIDString, queue.size(), maxQueueSize);
                // signal poll() to start draining queue and do not wait
                this.isFull.signalAll();
            }
        }
        finally {
            this.lock.unlock();
            long totalDuration = System.currentTimeMillis() - enqueueStartTime;
            if (waitCycles > 0 || totalDuration > 100) {
                LOGGER.info("[{} doEnqueue] Lock RELEASED - Enqueue completed in {}ms ({} wait cycles) - queue: {}/{}",
                        randomUUIDString, totalDuration, waitCycles, queue.size(), maxQueueSize);
            }
        }
    }

    /**
     * Returns the next batch of elements from this queue. May be empty in case no
     * elements have arrived in the maximum waiting time.
     *
     * @throws InterruptedException
     *             if this thread has been interrupted while waiting for more
     *             elements to arrive
     */
    public List<T> poll() throws InterruptedException {
        LoggingContext.PreviousContext previousContext = loggingContextSupplier.get();

        String pollUUID = UUID.randomUUID().toString();
        LOGGER.info("[{} poll] ========== POLL CALLED BY KAFKA CONNECT ==========", pollUUID);

        try {
            LOGGER.info("[{} poll] Attempting to acquire lock...", pollUUID);
            long startTime = System.currentTimeMillis();
            final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.min(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
            try {
                this.lock.lock();
                long lockAcquiredTime = System.currentTimeMillis();
                LOGGER.info("[{} poll] Lock ACQUIRED after {} ms - queue: {}/{}, bytes: {}/{}",
                        pollUUID, lockAcquiredTime - startTime, queue.size(), maxQueueSize,
                        currentQueueSizeInBytes, maxQueueSizeInBytes);

                List<T> records = new ArrayList<>(Math.min(maxBatchSize, queue.size()));
                throwProducerExceptionIfPresent();

                int drainAttempts = 0;
                while (drainRecords(records, maxBatchSize - records.size()) < maxBatchSize
                        && (maxQueueSizeInBytes == 0 || currentQueueSizeInBytes < maxQueueSizeInBytes)
                        && !timeout.expired()) {
                    drainAttempts++;
                    throwProducerExceptionIfPresent();

                    LOGGER.info("[{} poll] Drain attempt {} - not enough records yet (have: {}, want: {}), sleeping a bit...",
                            pollUUID, drainAttempts, records.size(), maxBatchSize);
                    long remainingTimeoutMills = timeout.remaining().toMillis();
                    if (remainingTimeoutMills > 0) {
                        // signal doEnqueue() to add more records
                        LOGGER.info("[{} poll] SIGNALING isNotFull.signalAll() - telling producer there's space (attempt {})",
                                pollUUID, drainAttempts);
                        this.isNotFull.signalAll();

                        // no records available or batch size not reached yet, so wait a bit
                        LOGGER.info("[{} poll] BEFORE isFull.await() - waiting {} ms for more records (attempt {})",
                                pollUUID, remainingTimeoutMills, drainAttempts);
                        long awaitStart = System.currentTimeMillis();
                        boolean wasSignaled = this.isFull.await(remainingTimeoutMills, TimeUnit.MILLISECONDS);
                        long awaitDuration = System.currentTimeMillis() - awaitStart;

                        if (wasSignaled) {
                            LOGGER.info("[{} poll] AFTER isFull.await() - SIGNALED by producer after {} ms (attempt {})",
                                    pollUUID, awaitDuration, drainAttempts);
                        } else {
                            LOGGER.info("[{} poll] AFTER isFull.await() - TIMEOUT after {} ms (attempt {}) - NO SIGNAL",
                                    pollUUID, awaitDuration, drainAttempts);
                        }
                    }
                    LOGGER.info("[{} poll] Checking for more records... (current: {}, target: {})",
                            pollUUID, records.size(), maxBatchSize);
                }

                // signal doEnqueue() to add more records
                LOGGER.info("[{} poll] FINAL SIGNAL - isNotFull.signalAll() to wake any blocked producers", pollUUID);
                this.isNotFull.signalAll();

                if (!records.isEmpty()) {
                    long duration = System.currentTimeMillis() - startTime;
                    LOGGER.info("[{} poll] PERF: Poll returned {} records in {}ms, rate: {}/sec",
                            pollUUID, records.size(), duration, records.size() * 1000.0 / duration);
                } else {
                    LOGGER.info("[{} poll] Returning EMPTY list - no records available", pollUUID);
                }

                return records;
            }
            finally {
                this.lock.unlock();
                long totalDuration = System.currentTimeMillis() - startTime;
                LOGGER.info("[{} poll] Lock RELEASED - poll operation completed in {} ms. Queue size: {}/{}, bytes: {}/{}",
                        pollUUID, totalDuration, queue.size(), maxQueueSize, currentQueueSizeInBytes, maxQueueSizeInBytes);
                LOGGER.info("[{} poll] ========== POLL COMPLETED ==========", pollUUID);
            }
        }
        finally {
            previousContext.restore();
        }
    }

    private long drainRecords(List<T> records, int maxElements) throws InterruptedException {
        int queueSize = queue.size();
        if (queueSize == 0) {
            return records.size();
        }
        int recordsToDrain = Math.min(queueSize, maxElements);
        LOGGER.info("Draining {} records from queue (current size: {})", recordsToDrain, queueSize);
        T[] drainedRecords = (T[]) new Sizeable[recordsToDrain];
        for (int i = 0; i < recordsToDrain; i++) {
            T record = queue.poll();
            drainedRecords[i] = record;
        }
        if (maxQueueSizeInBytes > 0) {
            for (int i = 0; i < recordsToDrain; i++) {
                Long objectSize = sizeInBytesQueue.poll();
                currentQueueSizeInBytes -= (objectSize == null ? 0L : objectSize);
            }
        }
        records.addAll(Arrays.asList(drainedRecords));
        LOGGER.info("Drained {} records from queue, new queue size: {}", records.size(), queue.size());
        return records.size();
    }

    public void producerException(final RuntimeException producerException) {
        this.producerException = producerException;
    }

    private void throwProducerExceptionIfPresent() {
        if (producerException != null) {
            LOGGER.info("Throwing producer exception to consumer: {}", producerException.getMessage());
            throw producerException;
        }
    }

    @Override
    public int totalCapacity() {
        return maxQueueSize;
    }

    @Override
    public int remainingCapacity() {
        return maxQueueSize - queue.size();
    }

    @Override
    public long maxQueueSizeInBytes() {
        return maxQueueSizeInBytes;
    }

    @Override
    public long currentQueueSizeInBytes() {
        return currentQueueSizeInBytes;
    }

    public boolean isBuffered() {
        return buffering;
    }
}
