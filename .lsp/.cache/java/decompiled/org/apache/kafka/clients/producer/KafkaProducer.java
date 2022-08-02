/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  java.io.Closeable
 *  java.lang.ClassCastException
 *  java.lang.Exception
 *  java.lang.IllegalArgumentException
 *  java.lang.IllegalStateException
 *  java.lang.Integer
 *  java.lang.InterruptedException
 *  java.lang.Long
 *  java.lang.Math
 *  java.lang.NumberFormatException
 *  java.lang.Object
 *  java.lang.Runnable
 *  java.lang.String
 *  java.lang.Thread
 *  java.lang.Throwable
 *  java.util.Collections
 *  java.util.List
 *  java.util.Map
 *  java.util.Objects
 *  java.util.Properties
 *  java.util.concurrent.Future
 *  java.util.concurrent.TimeUnit
 *  java.util.concurrent.atomic.AtomicInteger
 *  java.util.concurrent.atomic.AtomicReference
 *  org.apache.kafka.clients.ApiVersions
 *  org.apache.kafka.clients.ClientUtils
 *  org.apache.kafka.clients.KafkaClient
 *  org.apache.kafka.clients.Metadata
 *  org.apache.kafka.clients.NetworkClient
 *  org.apache.kafka.clients.consumer.OffsetAndMetadata
 *  org.apache.kafka.clients.producer.BufferExhaustedException
 *  org.apache.kafka.clients.producer.Callback
 *  org.apache.kafka.clients.producer.KafkaProducer$ClusterAndWaitTime
 *  org.apache.kafka.clients.producer.KafkaProducer$FutureFailure
 *  org.apache.kafka.clients.producer.KafkaProducer$InterceptorCallback
 *  org.apache.kafka.clients.producer.Partitioner
 *  org.apache.kafka.clients.producer.Producer
 *  org.apache.kafka.clients.producer.ProducerConfig
 *  org.apache.kafka.clients.producer.ProducerInterceptor
 *  org.apache.kafka.clients.producer.ProducerRecord
 *  org.apache.kafka.clients.producer.RecordMetadata
 *  org.apache.kafka.clients.producer.internals.ProducerInterceptors
 *  org.apache.kafka.clients.producer.internals.ProducerMetrics
 *  org.apache.kafka.clients.producer.internals.RecordAccumulator
 *  org.apache.kafka.clients.producer.internals.RecordAccumulator$RecordAppendResult
 *  org.apache.kafka.clients.producer.internals.Sender
 *  org.apache.kafka.clients.producer.internals.SenderMetricsRegistry
 *  org.apache.kafka.clients.producer.internals.TransactionManager
 *  org.apache.kafka.clients.producer.internals.TransactionalRequestResult
 *  org.apache.kafka.common.Cluster
 *  org.apache.kafka.common.KafkaException
 *  org.apache.kafka.common.Metric
 *  org.apache.kafka.common.MetricName
 *  org.apache.kafka.common.PartitionInfo
 *  org.apache.kafka.common.TopicPartition
 *  org.apache.kafka.common.config.AbstractConfig
 *  org.apache.kafka.common.config.ConfigException
 *  org.apache.kafka.common.errors.ApiException
 *  org.apache.kafka.common.errors.InterruptException
 *  org.apache.kafka.common.errors.ProducerFencedException
 *  org.apache.kafka.common.errors.RecordTooLargeException
 *  org.apache.kafka.common.errors.SerializationException
 *  org.apache.kafka.common.errors.TimeoutException
 *  org.apache.kafka.common.errors.TopicAuthorizationException
 *  org.apache.kafka.common.header.Header
 *  org.apache.kafka.common.header.Headers
 *  org.apache.kafka.common.header.internals.RecordHeaders
 *  org.apache.kafka.common.internals.ClusterResourceListeners
 *  org.apache.kafka.common.metrics.JmxReporter
 *  org.apache.kafka.common.metrics.MetricConfig
 *  org.apache.kafka.common.metrics.Metrics
 *  org.apache.kafka.common.metrics.MetricsReporter
 *  org.apache.kafka.common.metrics.Sensor
 *  org.apache.kafka.common.metrics.Sensor$RecordingLevel
 *  org.apache.kafka.common.network.ChannelBuilder
 *  org.apache.kafka.common.network.Selectable
 *  org.apache.kafka.common.network.Selector
 *  org.apache.kafka.common.record.AbstractRecords
 *  org.apache.kafka.common.record.CompressionType
 *  org.apache.kafka.common.serialization.ExtendedSerializer
 *  org.apache.kafka.common.serialization.ExtendedSerializer$Wrapper
 *  org.apache.kafka.common.serialization.Serializer
 *  org.apache.kafka.common.utils.AppInfoParser
 *  org.apache.kafka.common.utils.KafkaThread
 *  org.apache.kafka.common.utils.LogContext
 *  org.apache.kafka.common.utils.Time
 *  org.slf4j.Logger
 */
package org.apache.kafka.clients.producer;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

public class KafkaProducer<K, V>
implements Producer<K, V> {
    private final Logger log;
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    private final String clientId;
    final Metrics metrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final ExtendedSerializer<K> keySerializer;
    private final ExtendedSerializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    private TransactionalRequestResult initTransactionsResult;

    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null, null, null);
    }

    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)), keySerializer, valueSerializer, null, null);
    }

    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null, null, null);
    }

    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig((Properties)properties, keySerializer, valueSerializer)), keySerializer, valueSerializer, null, null);
    }

    KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer, Metadata metadata, KafkaClient kafkaClient) {
        try {
            Map userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = Time.SYSTEM;
            String clientId = config.getString("client.id");
            if (clientId.length() <= 0) {
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            }
            this.clientId = clientId;
            String transactionalId = userProvidedConfigs.containsKey((Object)"transactional.id") ? (String)userProvidedConfigs.get((Object)"transactional.id") : null;
            LogContext logContext = transactionalId == null ? new LogContext(String.format((String)"[Producer clientId=%s] ", (Object[])new Object[]{clientId})) : new LogContext(String.format((String)"[Producer clientId=%s, transactionalId=%s] ", (Object[])new Object[]{clientId, transactionalId}));
            this.log = logContext.logger(KafkaProducer.class);
            this.log.trace("Starting the Kafka producer");
            Map metricTags = Collections.singletonMap((Object)"client-id", (Object)clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt("metrics.num.samples").intValue()).timeWindow(config.getLong("metrics.sample.window.ms").longValue(), TimeUnit.MILLISECONDS).recordLevel(Sensor.RecordingLevel.forName((String)config.getString("metrics.recording.level"))).tags(metricTags);
            List reporters = config.getConfiguredInstances("metric.reporters", MetricsReporter.class);
            reporters.add((Object)new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, this.time);
            ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
            this.partitioner = (Partitioner)config.getConfiguredInstance("partitioner.class", Partitioner.class);
            long retryBackoffMs = config.getLong("retry.backoff.ms");
            if (keySerializer == null) {
                this.keySerializer = ExtendedSerializer.Wrapper.ensureExtended((Serializer)((Serializer)config.getConfiguredInstance("key.serializer", Serializer.class)));
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore("key.serializer");
                this.keySerializer = ExtendedSerializer.Wrapper.ensureExtended(keySerializer);
            }
            if (valueSerializer == null) {
                this.valueSerializer = ExtendedSerializer.Wrapper.ensureExtended((Serializer)((Serializer)config.getConfiguredInstance("value.serializer", Serializer.class)));
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore("value.serializer");
                this.valueSerializer = ExtendedSerializer.Wrapper.ensureExtended(valueSerializer);
            }
            userProvidedConfigs.put((Object)"client.id", (Object)clientId);
            List interceptorList = new ProducerConfig(userProvidedConfigs, false).getConfiguredInstances("interceptor.classes", ProducerInterceptor.class);
            this.interceptors = new ProducerInterceptors(interceptorList);
            ClusterResourceListeners clusterResourceListeners = this.configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, reporters);
            this.maxRequestSize = config.getInt("max.request.size");
            this.totalMemorySize = config.getLong("buffer.memory");
            this.compressionType = CompressionType.forName((String)config.getString("compression.type"));
            this.maxBlockTimeMs = config.getLong("max.block.ms");
            this.requestTimeoutMs = config.getInt("request.timeout.ms");
            this.transactionManager = KafkaProducer.configureTransactionState(config, logContext, this.log);
            int retries = KafkaProducer.configureRetries(config, this.transactionManager != null, this.log);
            int maxInflightRequests = KafkaProducer.configureInflightRequests(config, this.transactionManager != null);
            short acks = KafkaProducer.configureAcks(config, this.transactionManager != null, this.log);
            this.apiVersions = new ApiVersions();
            this.accumulator = new RecordAccumulator(logContext, config.getInt("batch.size").intValue(), this.totalMemorySize, this.compressionType, config.getLong("linger.ms").longValue(), retryBackoffMs, this.metrics, this.time, this.apiVersions, this.transactionManager);
            List addresses = ClientUtils.parseAndValidateAddresses((List)config.getList("bootstrap.servers"));
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new Metadata(retryBackoffMs, config.getLong("metadata.max.age.ms").longValue(), true, true, clusterResourceListeners);
                this.metadata.update(Cluster.bootstrap((List)addresses), Collections.emptySet(), this.time.milliseconds());
            }
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder((AbstractConfig)config);
            Sensor throttleTimeSensor = Sender.throttleTimeSensor((SenderMetricsRegistry)metricsRegistry.senderMetrics);
            KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient((Selectable)new Selector(config.getLong("connections.max.idle.ms").longValue(), this.metrics, this.time, "producer", channelBuilder, logContext), this.metadata, clientId, maxInflightRequests, config.getLong("reconnect.backoff.ms").longValue(), config.getLong("reconnect.backoff.max.ms").longValue(), config.getInt("send.buffer.bytes").intValue(), config.getInt("receive.buffer.bytes").intValue(), this.requestTimeoutMs, this.time, true, this.apiVersions, throttleTimeSensor, logContext);
            this.sender = new Sender(logContext, client, this.metadata, this.accumulator, maxInflightRequests == 1, config.getInt("max.request.size").intValue(), acks, retries, metricsRegistry.senderMetrics, Time.SYSTEM, this.requestTimeoutMs, config.getLong("retry.backoff.ms").longValue(), this.transactionManager, this.apiVersions);
            String ioThreadName = "kafka-producer-network-thread | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, (Runnable)this.sender, true);
            this.ioThread.start();
            this.errors = this.metrics.sensor("errors");
            config.logUnused();
            AppInfoParser.registerAppInfo((String)JMX_PREFIX, (String)clientId, (Metrics)this.metrics);
            this.log.debug("Kafka producer started");
        }
        catch (Throwable t) {
            this.close(0L, TimeUnit.MILLISECONDS, true);
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext, Logger log) {
        boolean idempotenceEnabled;
        TransactionManager transactionManager = null;
        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey((Object)"enable.idempotence")) {
            userConfiguredIdempotence = true;
        }
        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey((Object)"transactional.id")) {
            userConfiguredTransactions = true;
        }
        if (!(idempotenceEnabled = config.getBoolean("enable.idempotence").booleanValue()) && userConfiguredIdempotence && userConfiguredTransactions) {
            throw new ConfigException("Cannot set a transactional.id without also enabling idempotence.");
        }
        if (userConfiguredTransactions) {
            idempotenceEnabled = true;
        }
        if (idempotenceEnabled) {
            long retryBackoffMs;
            int transactionTimeoutMs;
            String transactionalId = config.getString("transactional.id");
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs = config.getInt("transaction.timeout.ms").intValue(), retryBackoffMs = config.getLong("retry.backoff.ms").longValue());
            if (transactionManager.isTransactional()) {
                log.info("Instantiated a transactional producer.");
            } else {
                log.info("Instantiated an idempotent producer.");
            }
        }
        return transactionManager;
    }

    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        if (config.originals().containsKey((Object)"retries")) {
            userConfiguredRetries = true;
        }
        if (idempotenceEnabled && !userConfiguredRetries) {
            log.info("Overriding the default retries config to the recommended value of {} since the idempotent producer is enabled.", (Object)Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt("retries") == 0) {
            throw new ConfigException("Must set retries to non-zero when using the idempotent producer.");
        }
        return config.getInt("retries");
    }

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt("max.in.flight.requests.per.connection")) {
            throw new ConfigException("Must set max.in.flight.requests.per.connection to at most 5 to use the idempotent producer.");
        }
        return config.getInt("max.in.flight.requests.per.connection");
    }

    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        short acks = (short)KafkaProducer.parseAcks(config.getString("acks"));
        if (config.originals().containsKey((Object)"acks")) {
            userConfiguredAcks = true;
        }
        if (idempotenceEnabled && !userConfiguredAcks) {
            log.info("Overriding the default {} to all since idempotence is enabled.", (Object)"acks");
            return -1;
        }
        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt((String)acksString.trim());
        }
        catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    public void initTransactions() {
        this.throwIfNoTransactionManager();
        if (this.initTransactionsResult == null) {
            this.initTransactionsResult = this.transactionManager.initializeTransactions();
            this.sender.wakeup();
        }
        try {
            if (!this.initTransactionsResult.await(this.maxBlockTimeMs, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Timeout expired while initializing transactional state in " + this.maxBlockTimeMs + "ms.");
            }
            this.initTransactionsResult = null;
        }
        catch (InterruptedException e) {
            throw new InterruptException("Initialize transactions interrupted.", e);
        }
    }

    public void beginTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        this.transactionManager.beginTransaction();
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        TransactionalRequestResult result = this.transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        this.sender.wakeup();
        result.await();
    }

    public void commitTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        TransactionalRequestResult result = this.transactionManager.beginCommit();
        this.sender.wakeup();
        result.await();
    }

    public void abortTransaction() throws ProducerFencedException {
        this.throwIfNoTransactionManager();
        TransactionalRequestResult result = this.transactionManager.beginAbort();
        this.sender.wakeup();
        result.await();
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return this.send(record, null);
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        ProducerRecord interceptedRecord = this.interceptors.onSend(record);
        return this.doSend(interceptedRecord, callback);
    }

    private void throwIfProducerClosed() {
        if (this.ioThread == null || !this.ioThread.isAlive()) {
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
        }
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            byte[] serializedValue;
            byte[] serializedKey;
            ClusterAndWaitTime clusterAndWaitTime;
            this.throwIfProducerClosed();
            try {
                clusterAndWaitTime = this.waitOnMetadata(record.topic(), record.partition(), this.maxBlockTimeMs);
            }
            catch (KafkaException e) {
                if (this.metadata.isClosed()) {
                    throw new KafkaException("Producer closed while send in progress", (Throwable)e);
                }
                throw e;
            }
            long remainingWaitMs = Math.max((long)0L, (long)(this.maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs));
            Cluster cluster = clusterAndWaitTime.cluster;
            try {
                serializedKey = this.keySerializer.serialize(record.topic(), record.headers(), record.key());
            }
            catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() + " to class " + this.producerConfig.getClass("key.serializer").getName() + " specified in key.serializer", (Throwable)cce);
            }
            try {
                serializedValue = this.valueSerializer.serialize(record.topic(), record.headers(), record.value());
            }
            catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() + " to class " + this.producerConfig.getClass("value.serializer").getName() + " specified in value.serializer", (Throwable)cce);
            }
            int partition = this.partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);
            this.setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound((byte)this.apiVersions.maxUsableProduceMagic(), (CompressionType)this.compressionType, (byte[])serializedKey, (byte[])serializedValue, (Header[])headers);
            this.ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? this.time.milliseconds() : record.timestamp().longValue();
            this.log.trace("Sending record {} with callback {} to topic {} partition {}", new Object[]{record, callback, record.topic(), partition});
            InterceptorCallback interceptCallback = new InterceptorCallback(callback, this.interceptors, tp, null);
            if (this.transactionManager != null && this.transactionManager.isTransactional()) {
                this.transactionManager.maybeAddPartitionToTransaction(tp);
            }
            RecordAccumulator.RecordAppendResult result = this.accumulator.append(tp, timestamp, serializedKey, serializedValue, headers, (Callback)interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                this.log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", (Object)record.topic(), (Object)partition);
                this.sender.wakeup();
            }
            return result.future;
        }
        catch (ApiException e) {
            this.log.debug("Exception occurred during message send:", (Throwable)e);
            if (callback != null) {
                callback.onCompletion(null, (Exception)((Object)e));
            }
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            return new FutureFailure((Exception)((Object)e));
        }
        catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            throw new InterruptException(e);
        }
        catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            throw e;
        }
        catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, (Exception)((Object)e));
            throw e;
        }
        catch (Exception e) {
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders)headers).setReadOnly();
        }
    }

    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
        long elapsed;
        this.metadata.add(topic);
        Cluster cluster = this.metadata.fetch();
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        if (partitionsCount != null && (partition == null || partition < partitionsCount)) {
            return new ClusterAndWaitTime(cluster, 0L);
        }
        long begin = this.time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        do {
            this.log.trace("Requesting metadata update for topic {}.", (Object)topic);
            this.metadata.add(topic);
            int version = this.metadata.requestUpdate();
            this.sender.wakeup();
            try {
                this.metadata.awaitUpdate(version, remainingWaitMs);
            }
            catch (TimeoutException ex) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            cluster = this.metadata.fetch();
            elapsed = this.time.milliseconds() - begin;
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            if (cluster.unauthorizedTopics().contains((Object)topic)) {
                throw new TopicAuthorizationException(topic);
            }
            remainingWaitMs = maxWaitMs - elapsed;
        } while ((partitionsCount = cluster.partitionCountForTopic(topic)) == null);
        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(String.format((String)"Invalid partition given with record: %d is not in the range [0...%d).", (Object[])new Object[]{partition, partitionsCount}));
        }
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize) {
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than the maximum request size you have configured with the " + "max.request.size" + " configuration.");
        }
        if ((long)size > this.totalMemorySize) {
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than the total memory buffer you have configured with the " + "buffer.memory" + " configuration.");
        }
    }

    public void flush() {
        this.log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        }
        catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull((Object)topic, (String)"topic cannot be null");
        try {
            return this.waitOnMetadata((String)topic, null, (long)this.maxBlockTimeMs).cluster.partitionsForTopic(topic);
        }
        catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap((Map)this.metrics.metrics());
    }

    public void close() {
        this.close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void close(long timeout, TimeUnit timeUnit) {
        this.close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        boolean invokedFromCallback;
        if (timeout < 0L) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }
        this.log.info("Closing the Kafka producer with timeoutMillis = {} ms.", (Object)timeUnit.toMillis(timeout));
        AtomicReference firstException = new AtomicReference();
        boolean bl = invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0L) {
            if (invokedFromCallback) {
                this.log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", (Object)timeout);
            } else {
                if (this.sender != null) {
                    this.sender.initiateClose();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    }
                    catch (InterruptedException t) {
                        firstException.compareAndSet(null, (Object)new InterruptException(t));
                        this.log.error("Interrupted while joining ioThread", (Throwable)t);
                    }
                }
            }
        }
        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            this.log.info("Proceeding to force close the producer since pending requests could not be completed within timeout {} ms.", (Object)timeout);
            this.sender.forceClose();
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                }
                catch (InterruptedException e) {
                    firstException.compareAndSet(null, (Object)new InterruptException(e));
                }
            }
        }
        ClientUtils.closeQuietly(this.interceptors, (String)"producer interceptors", (AtomicReference)firstException);
        ClientUtils.closeQuietly((Closeable)this.metrics, (String)"producer metrics", (AtomicReference)firstException);
        ClientUtils.closeQuietly(this.keySerializer, (String)"producer keySerializer", (AtomicReference)firstException);
        ClientUtils.closeQuietly(this.valueSerializer, (String)"producer valueSerializer", (AtomicReference)firstException);
        ClientUtils.closeQuietly((Closeable)this.partitioner, (String)"producer partitioner", (AtomicReference)firstException);
        AppInfoParser.unregisterAppInfo((String)JMX_PREFIX, (String)this.clientId, (Metrics)this.metrics);
        this.log.debug("Kafka producer has been closed");
        Throwable exception = (Throwable)((Object)firstException.get());
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException)exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?> ... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists) {
            clusterResourceListeners.maybeAddAll(candidateList);
        }
        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ? partition.intValue() : this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (this.transactionManager == null) {
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property");
        }
    }
}
