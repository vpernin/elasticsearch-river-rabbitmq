/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final Address[] rabbitAddresses;
    private final String rabbitUser;
    private final String rabbitPassword;
    private final String rabbitVhost;

    private final String rabbitQueue;
    private final String rabbitExchange;
    private final String rabbitExchangeType;
    private final String rabbitRoutingKey;
    private final boolean rabbitExchangeDurable;
    private final boolean rabbitQueueDurable;
    private final boolean rabbitQueueAutoDelete;
    private Map rabbitQueueArgs = null; //extra arguments passed to queue for creation (ha settings for example)

    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    protected enum PercolateMode {
        /**
         * No percolation
         */
        DISABLED,

        /**
         * Add _percolate to each index action in the bulk request and index the percolator match results
         */
        ON_THE_FLY,

        /**
         * Percolate against an alias is not possible, so index the bulk request,
         * then percolate each index action against a specific index, then index the percolator match results
         */
        INDEX_THEN_PERCOLATE
    }

    /**
     * The percolate mode choosen, DISABLED by default
     */
    private PercolateMode percolateMode;

    /**
     * Percolate value used with <code>PercolateMode.ON_THE_FLY</code>
     * if the bulk index request does not contain percolate parameter,
     * Defaults to *
     */
    private String percolateQuery;

    /**
     * The index to percolate against with <code>PercolateMode.INDEX_THEN_PERCOLATE</code>
     */
    private String percolateIndex;

    /**
     * The percolator match results will be stored in this index
     */
    private String percolateMatchIndex;

    /**
     * The percolator match results will be stored under this type
     */
    private String percolateMatchType;


    @SuppressWarnings({"unchecked"})
    @Inject
    public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("rabbitmq")) {
            Map<String, Object> rabbitSettings = (Map<String, Object>) settings.settings().get("rabbitmq");

            if (rabbitSettings.containsKey("addresses")) {
                List<Address> addresses = new ArrayList<Address>();
                for(Map<String, Object> address : (List<Map<String, Object>>) rabbitSettings.get("addresses")) {
                    addresses.add( new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"),
                            XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
                }
                rabbitAddresses = addresses.toArray(new Address[addresses.size()]);
            } else {
                String rabbitHost = XContentMapValues.nodeStringValue(rabbitSettings.get("host"), "localhost");
                int rabbitPort = XContentMapValues.nodeIntegerValue(rabbitSettings.get("port"), AMQP.PROTOCOL.PORT);
                rabbitAddresses = new Address[]{ new Address(rabbitHost, rabbitPort) };
            }

            rabbitUser = XContentMapValues.nodeStringValue(rabbitSettings.get("user"), "guest");
            rabbitPassword = XContentMapValues.nodeStringValue(rabbitSettings.get("pass"), "guest");
            rabbitVhost = XContentMapValues.nodeStringValue(rabbitSettings.get("vhost"), "/");

            rabbitQueue = XContentMapValues.nodeStringValue(rabbitSettings.get("queue"), "elasticsearch");
            rabbitExchange = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange"), "elasticsearch");
            rabbitExchangeType = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange_type"), "direct");
            rabbitRoutingKey = XContentMapValues.nodeStringValue(rabbitSettings.get("routing_key"), "elasticsearch");
            rabbitExchangeDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_durable"), true);
            rabbitQueueDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_durable"), true);
            rabbitQueueAutoDelete = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_auto_delete"), false);

            if (rabbitSettings.containsKey("args")) {
                rabbitQueueArgs = (Map<String, Object>) rabbitSettings.get("args");
            }
        } else {
            rabbitAddresses = new Address[]{ new Address("localhost", AMQP.PROTOCOL.PORT) };
            rabbitUser = "guest";
            rabbitPassword = "guest";
            rabbitVhost = "/";

            rabbitQueue = "elasticsearch";
            rabbitQueueAutoDelete = false;
            rabbitQueueDurable = true;
            rabbitExchange = "elasticsearch";
            rabbitExchangeType = "direct";
            rabbitExchangeDurable = true;
            rabbitRoutingKey = "elasticsearch";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            ordered = false;
        }

        if (settings.settings().containsKey("percolate")) {
            Map<String, Object> percolateSettings = (Map<String, Object>) settings.settings().get("percolate");
            if (percolateSettings.containsKey("percolate_mode")) {
                String percolateModeValue = XContentMapValues.nodeStringValue(percolateSettings.get("percolate_mode"), PercolateMode.DISABLED.name());
                percolateMode = PercolateMode.valueOf(percolateModeValue);

                // We do not have access to the index request from the bulk index response when using asynchronous execution,
                //  so the only possible mode is to use ON_THE_FLY mode
                if (percolateMode.equals(PercolateMode.INDEX_THEN_PERCOLATE) && !ordered) {
                    logger.error("Percolate mode INDEX_THEN_PERCOLATE is not possible with non ordered, switching to ON_THE_FLY mode");
                    percolateMode = PercolateMode.ON_THE_FLY;
                }
            }
            percolateQuery = XContentMapValues.nodeStringValue(percolateSettings.get("percolate_query"), "*");
            percolateIndex = XContentMapValues.nodeStringValue(percolateSettings.get("percolate_index"), "percolate_index");
            percolateMatchIndex = XContentMapValues.nodeStringValue(percolateSettings.get("percolate_match_index"), "percolate_match");
            percolateMatchType = XContentMapValues.nodeStringValue(percolateSettings.get("percolate_match_type"), "percolate");
        } else {
            percolateMode = PercolateMode.DISABLED;
        }
    }

    @Override
    public void start() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(rabbitUser);
        connectionFactory.setPassword(rabbitPassword);
        connectionFactory.setVirtualHost(rabbitVhost);

        logger.info("creating rabbitmq river, addresses [{}], user [{}], vhost [{}]", rabbitAddresses, connectionFactory.getUsername(), connectionFactory.getVirtualHost());
        logger.info("rabbitmq river percolator, mode [{}], query [{}], percolate index [{}], match index [{}], match type [{}]",
                percolateMode, percolateQuery, percolateIndex, percolateMatchIndex, percolateMatchType);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "rabbitmq_river").newThread(new Consumer());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing rabbitmq river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private Connection connection;

        private Channel channel;

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    break;
                }

                try {
                    connection = connectionFactory.newConnection(rabbitAddresses);
                    channel = connection.createChannel();
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                QueueingConsumer consumer = new QueueingConsumer(channel);
                // define the queue
                try {
                    channel.exchangeDeclare(rabbitExchange/*exchange*/, rabbitExchangeType/*type*/, rabbitExchangeDurable);
                    channel.queueDeclare(rabbitQueue/*queue*/, rabbitQueueDurable/*durable*/, false/*exclusive*/, rabbitQueueAutoDelete/*autoDelete*/, rabbitQueueArgs/*extra args*/);
                    channel.queueBind(rabbitQueue/*queue*/, rabbitExchange/*exchange*/, rabbitRoutingKey/*routingKey*/);
                    channel.basicConsume(rabbitQueue/*queue*/, false/*noAck*/, consumer);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue [{}]", e, rabbitQueue);
                    }
                    cleanup(0, "failed to create queue");
                    continue;
                }

                // now use the queue to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    QueueingConsumer.Delivery task;
                    try {
                        task = consumer.nextDelivery();
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }

                    if (task != null && task.getBody() != null) {
                        final List<Long> deliveryTags = Lists.newArrayList();

                        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                        try {
                            bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                        } catch (Exception e) {
                            logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                            try {
                                channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                            } catch (IOException e1) {
                                logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                            }
                            continue;
                        }

                        deliveryTags.add(task.getEnvelope().getDeliveryTag());

                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
                                while ((task = consumer.nextDelivery(bulkTimeout.millis())) != null) {
                                    try {
                                        bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                                        deliveryTags.add(task.getEnvelope().getDeliveryTag());
                                    } catch (Exception e) {
                                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                                        try {
                                            channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                                        } catch (Exception e1) {
                                            logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
                                        }
                                    }
                                    if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                                        break;
                                    }
                                }
                            } catch (InterruptedException e) {
                                if (closed) {
                                    break;
                                }
                            }
                        }

                        // Add percolate on the fly if required
                        if (percolateMode.equals(PercolateMode.ON_THE_FLY)) {
                            percolateRequestsOnTheFly(bulkRequestBuilder);
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                        }

                        if (ordered) {
                            BulkResponse response =  null;
                            try {
                                response = bulkRequestBuilder.execute().actionGet();
                                if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }

                                for (Long deliveryTag : deliveryTags) {
                                    try {
                                        channel.basicAck(deliveryTag, false);
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack [{}]", e1, deliveryTag);
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to execute bulk", e);
                            }

                            try {
                                if (percolateMode.equals(PercolateMode.ON_THE_FLY)) {
                                    handlePercolateMatch(response);
                                } else if (percolateMode.equals(PercolateMode.INDEX_THEN_PERCOLATE)) {
                                    percolateResponses(bulkRequestBuilder, response);
                                }
                            } catch (Exception e) {
                                logger.warn("failed to percolate", e);
                            }


                        } else {
                            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                                @Override
                                public void onResponse(BulkResponse response) {
                                    if (response.hasFailures()) {
                                        // TODO write to exception queue?
                                        logger.warn("failed to execute" + response.buildFailureMessage());
                                    }
                                    for (Long deliveryTag : deliveryTags) {
                                        try {
                                            channel.basicAck(deliveryTag, false);
                                        } catch (Exception e1) {
                                            logger.warn("failed to ack [{}]", e1, deliveryTag);
                                        }
                                    }

                                    if (percolateMode.equals(PercolateMode.ON_THE_FLY)) {
                                        try {
                                            handlePercolateMatch(response);
                                        } catch (Exception e) {
                                            logger.warn("failed to index percolate match", e);
                                        }
                                    }
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    logger.warn("failed to execute bulk for delivery tags [{}], not ack'ing", e, deliveryTags);
                                }
                            });
                        }
                    }
                }
            }
            cleanup(0, "closing river");
        }

        private void cleanup(int code, String message) {
            try {
                channel.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close channel on [{}]", e, message);
            }
            try {
                connection.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close connection on [{}]", e, message);
            }
        }

        /**
         * If percolate on the fly is enabled, for each index action in the bulk request,<br/>
         * if percolation is not detected, add percolate parameter to the request with <code>percolateQuery</code>
         *
         * @param bulkRequestBuilder the bulk request just built ready to be indexed
         */
        private void percolateRequestsOnTheFly(BulkRequestBuilder bulkRequestBuilder) {
            for (ActionRequest actionRequest : bulkRequestBuilder.request().requests()) {
                if (actionRequest instanceof IndexRequest) {

                    IndexRequest indexRequest = (IndexRequest) actionRequest;
                    if (indexRequest.percolate() == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Adding percolate to bulk request [{}]", indexRequest);
                        }
                        indexRequest.percolate(percolateQuery);
                    }
                }
            }
        }

        /**
         * If index then percolate mode is enabled, percolate each request in the bulkRequestBuilder parameter <br />
         * against <code>percolatorIndex</code> and returns the response.
         *
         * @param bulkRequestBuilder the builder containing the request to iterate over to percolate each element
         * @param bulkRequestResponse the bulk response
         */
        private void percolateResponses(BulkRequestBuilder bulkRequestBuilder, BulkResponse bulkRequestResponse) throws IOException {
            BulkRequestBuilder matchRequests = client.prepareBulk();
            String percolateDate = ISODateTimeFormat.dateTime().print(System.currentTimeMillis());

            List<ActionRequest> requests = bulkRequestBuilder.request().requests();
            for (int i = 0 ; i < requests.size() ; i++) {
                ActionRequest actionRequest = requests.get(i);
                if (actionRequest instanceof IndexRequest) {

                    IndexRequest indexRequest = (IndexRequest) actionRequest;

                    // Need to find a way not to use a dummy field as first json objet to be able to use the raw indexRequest source
                    XContentBuilder docBuilder = XContentFactory.jsonBuilder()
                            .startObject()
                                .field("dummy_field", "dummy_value")
                                .rawField("doc", indexRequest.source().toBytes())
                            .endObject();

                    PercolateResponse percolateResponse = client.preparePercolate(percolateIndex, indexRequest.type())
                            .setSource(docBuilder)
                            .execute().actionGet();
                    BulkItemResponse bulkItemResponse = bulkRequestResponse.items()[i];

                    indexPercolatorMatch(matchRequests, indexRequest.index(), indexRequest.type(),
                            bulkItemResponse.id(), percolateDate, percolateResponse.matches());
                }
            }

            if (matchRequests.numberOfActions() > 0) {

                BulkResponse bulkResponse = matchRequests.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    logger.error("Failures while adding log alert " + bulkResponse.buildFailureMessage());
                }
            }
        }

        /**
         * If percolate on the fly is enabled, for each response in the bulk response that match a percolator query,<br/>
         * index the match as specified by <code>percolateMatchIndex</code>, <code>percolateMatchType</code> parameters
         *
         * @param bulkRequestResponse the bulk response that may contains percolator matched
         */
        private void handlePercolateMatch(BulkResponse bulkRequestResponse) {
            BulkRequestBuilder matchRequests = client.prepareBulk();
            String percolateDate = ISODateTimeFormat.dateTime().print(System.currentTimeMillis());

            for (BulkItemResponse response : bulkRequestResponse.items()) {
                if (response.response() instanceof IndexResponse) {
                    IndexResponse indexResponse = (IndexResponse) response.response();

                    indexPercolatorMatch(matchRequests, indexResponse.getIndex(), indexResponse.getType(),
                            indexResponse.getId(), percolateDate, indexResponse.getMatches());
                }
            }

            if (matchRequests.numberOfActions() > 0) {

                BulkResponse bulkResponse = matchRequests.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    logger.error("Failures while adding log alert " + bulkResponse.buildFailureMessage());
                }
            }
        }

        private void indexPercolatorMatch(BulkRequestBuilder matchRequests, String index, String type, String id, String percolateDate, List<String> matches) {
            if (matches == null || matches.isEmpty()) {
                return;
            }

            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding to [index={},type={}] percolate match [index={},type={},id={},query={}]",
                            percolateMatchIndex, percolateMatchType,
                            index, type, id, matches);
                }

                matchRequests.add(client.prepareIndex(percolateMatchIndex, percolateMatchType)
                        .setSource(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("percolate_index", index)
                                .field("percolate_type", type)
                                .field("percolate_id", id)
                                .field("percolate_date", percolateDate)
                                .field("percolate_match", matches)
                                .endObject()
                        )
                );
            } catch (IOException e) {
                logger.error("Failures while building percolate match " + e.getMessage(), e);
            }
        }
    }
}
