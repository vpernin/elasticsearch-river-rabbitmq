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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import junit.framework.Assert;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.river.rabbitmq.RabbitmqRiver.PercolateMode.INDEX_THEN_PERCOLATE;
import static org.elasticsearch.river.rabbitmq.RabbitmqRiver.PercolateMode.ON_THE_FLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests of the RabbitMQ river.
 * JUnit test is commited ignored because it requires a running RabbitMQ on localhost.
 */
public class RabbitMQRiverTest {

    private static final String EXCHANGE_QUEUE_STANDARD = "elasticsearch";
    private static final String EXCHANGE_QUEUE_ON_THE_FLY = "elasticsearch-" + ON_THE_FLY;
    private static final String EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED = "elasticsearch-" + ON_THE_FLY + "-nonordered";
    private static final String EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE = "elasticsearch-" + INDEX_THEN_PERCOLATE;

    private static Node node;
    private static Client client;

    private static long publishDate;

    private static final String INDEX_TEST = "test";
    private static final String TYPE_TEST = "type1";

    private static final String PERCOLATOR1_ON_THE_FLY = "percolator1-otf";
    private static final String PERCOLATOR2_ON_THE_FLY = "percolator2-otf";

    private static final String PERCOLATE_INDEX = "percolate_index";
    private static final String PERCOLATOR1_INDEX_THE_PERCOLATE = "percolator1-itp";
    private static final String PERCOLATOR2_INDEX_THE_PERCOLATE = "percolator2-itp";

    @BeforeClass
    public static void setUp() throws IOException {
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();
        client = node.client();

        // Clean
        client.admin().indices().prepareDelete(new String[]{}).execute().actionGet();

        // Standard river
        createRiver(EXCHANGE_QUEUE_STANDARD, null, true);

        // River with ON_THE_FLY mode
        createRiver(EXCHANGE_QUEUE_ON_THE_FLY, ON_THE_FLY, true);

        // River with ON_THE_FLY mode, non ordered river
        createRiver(EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, ON_THE_FLY, false);

        // River with INDEX_THEN_PERCOLATE mode
        createRiver(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, INDEX_THEN_PERCOLATE, true);

        // Two percolator against index test1 for ON_THE_FLY mode
        createPercolator(client, INDEX_TEST, PERCOLATOR1_ON_THE_FLY, "field1", "value1");
        createPercolator(client, INDEX_TEST, PERCOLATOR2_ON_THE_FLY, "field2", "value2");

        // First percolator against index percolator_index for INDEX_THEN_PERCOLATE mode
        createPercolator(client, PERCOLATE_INDEX, PERCOLATOR1_INDEX_THE_PERCOLATE, "field1", "value1");
        createPercolator(client, PERCOLATE_INDEX, PERCOLATOR2_INDEX_THE_PERCOLATE, "field2", "value2");

        // Register an index to percolate against
        client.prepareIndex(PERCOLATE_INDEX, TYPE_TEST)
                .setSource(XContentFactory.jsonBuilder().startObject().field("dummy", "dummy").endObject())
                .execute().actionGet();

        // Declare RabbitMQ artefacts, one exchange/queue per percolate mode
        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setPort(AMQP.PROTOCOL.PORT);
        Connection conn = cfconn.newConnection();
        Channel ch = conn.createChannel();

        // Standard mode, no percolation
        ch.exchangeDeclare("elasticsearch", "direct", true);
        ch.queueDeclare("elasticsearch", true, false, false, null);

        // Percolation on the fly
        ch.exchangeDeclare(EXCHANGE_QUEUE_ON_THE_FLY, "direct", true);
        ch.queueDeclare(EXCHANGE_QUEUE_ON_THE_FLY, true, false, false, null);

        // Percolation on the fly non ordered
        ch.exchangeDeclare(EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, "direct", true);
        ch.queueDeclare(EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, true, false, false, null);

        // Index then percolate
        ch.exchangeDeclare(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, "direct", true);
        ch.queueDeclare(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, true, false, false, null);

        // Standard request
        String message = "{ \"index\" : { \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"1\" }\n" +
                "{\"field1\" : \"value1\" }\n" +
                "{ \"delete\" : { \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"2\" } }\n" +
                "{ \"create\" : { \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"1\" }\n" +
                "{ \"field1\" : \"value1\" }";

        // Percolated requests for ON_THE_FLY mode
        String percolate_onTheFly_message_percolateToAdd_oneMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"3\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        String percolate_onTheFly_message_oneMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"4\", \"_percolate\" : \"*\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        String percolate_onTheFly_message_twoMatches = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"5\", \"_percolate\" : \"*\" }\n" +
                "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }\n";

        String percolate_onTheFly_message_doesNotExist_percolator = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"-1\", \"_percolate\" : \"doesNotExist\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        String percolate_onTheFly_message_noMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"-2\", \"_percolate\" : \"*\" }\n" +
                "{ \"field1\" : \"wrongValue\" }\n";

        String percolate_onTheFly_message_wrong_percolator = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"-3\", \"_percolate\" : \"" + PERCOLATOR2_ON_THE_FLY + "\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        // Percolated requests for ON_THE_FLY mode, non ordered river
        String percolate_onTheFly_nonOrdered_message_oneMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"6\", \"_percolate\" : \"*\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        String percolate_onTheFly_nonOrdered_message_twoMatches = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"7\", \"_percolate\" : \"*\" }\n" +
                "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }\n";

        // Percolated requests for INDEX_THEN_PERCOLATE mode
        String index_then_percolate_oneMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"8\" }\n" +
                "{ \"field1\" : \"value1\" }\n";

        String index_then_percolate_twoMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"9\" }\n" +
                "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }\n";

        String index_then_percolate_noMatch = "{ \"index\" : " +
                "{ \"_index\" : \"" + INDEX_TEST + "\", \"_type\" : \"" + TYPE_TEST + "\", \"_id\" : \"-10\" }\n" +
                "{ \"field1\" : \"wrongValue\" }\n";

        publishDate = System.currentTimeMillis();

        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_percolateToAdd_oneMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_oneMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_twoMatches.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_doesNotExist_percolator.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_noMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY, EXCHANGE_QUEUE_ON_THE_FLY, null, percolate_onTheFly_message_wrong_percolator.getBytes());

        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, null, percolate_onTheFly_nonOrdered_message_oneMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, EXCHANGE_QUEUE_ON_THE_FLY_NON_ORDERED, null, percolate_onTheFly_nonOrdered_message_twoMatches.getBytes());

        ch.basicPublish(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, null, index_then_percolate_oneMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, null, index_then_percolate_twoMatch.getBytes());
        ch.basicPublish(EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, EXCHANGE_QUEUE_INDEX_THEN_PERCOLATE, null, index_then_percolate_noMatch.getBytes());

        ch.close();
        conn.close();
    }

    @Test
    public void testRiver() throws InterruptedException {
        Thread.sleep(5000);

        // Check if the default index holding percolator match contains our request
        SearchResponse searchResponse = client.prepareSearch("percolate_match")
                .setQuery(matchAllQuery())
                .addSort("percolate_id", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().hits().length, equalTo(7));
        assertPercolateMatch(searchResponse.hits().getAt(0), "3", Arrays.asList(PERCOLATOR1_ON_THE_FLY));
        assertPercolateMatch(searchResponse.hits().getAt(1), "4", Arrays.asList(PERCOLATOR1_ON_THE_FLY));
        assertPercolateMatch(searchResponse.hits().getAt(2), "5", Arrays.asList(PERCOLATOR1_ON_THE_FLY, PERCOLATOR2_ON_THE_FLY));

        assertPercolateMatch(searchResponse.hits().getAt(3), "6", Arrays.asList(PERCOLATOR1_ON_THE_FLY));
        assertPercolateMatch(searchResponse.hits().getAt(4), "7", Arrays.asList(PERCOLATOR1_ON_THE_FLY, PERCOLATOR2_ON_THE_FLY));

        assertPercolateMatch(searchResponse.hits().getAt(5), "8", Arrays.asList(PERCOLATOR1_INDEX_THE_PERCOLATE));
        assertPercolateMatch(searchResponse.hits().getAt(6), "9", Arrays.asList(PERCOLATOR1_INDEX_THE_PERCOLATE, PERCOLATOR2_INDEX_THE_PERCOLATE));
    }

    /**
     * Check that the search hit response has been percolated properly
     */
    @SuppressWarnings("unchecked")
    private static void assertPercolateMatch(SearchHit hit, String id, List<String> matches) {
        assertThat(hit.type(), equalTo("percolate"));
        assertThat(hit.getSource().get("percolate_index").toString(), equalTo(INDEX_TEST));
        assertThat(hit.getSource().get("percolate_type").toString(), equalTo(TYPE_TEST));
        assertThat(hit.getSource().get("percolate_id").toString(), equalTo(id));
        Assert.assertTrue(ISODateTimeFormat.dateTime().parseDateTime(hit.getSource().get("percolate_date").toString()).isAfter(publishDate));
        Assert.assertTrue(areListEquals(matches, (List<String>) hit.getSource().get("percolate_match")));
    }

    /**
     * Check that both list1 and list2 are equals
     */
    private static boolean areListEquals(List<String> list1, List<String> list2) {
        boolean match = true;

        if (list1.size() != list2.size()) {
            match = false;
        } else {
            for (String element : list1) {
                if (!list2.contains(element)) {
                    match = false;
                    break;
                }
            }
            for (String element : list2) {
                if (!list1.contains(element)) {
                    match = false;
                    break;
                }
            }
        }

        return match;
    }

    /**
     * Create a percolator
     */
    private static void createPercolator(Client client, String percolateIndex, String percolatorName, String queryField, String queryValue) throws IOException {
        client
                .prepareIndex("_percolator", percolateIndex, percolatorName)
                .setSource(jsonBuilder()
                        .startObject()
                            .startObject("query")
                                .startObject("term").field(queryField, queryValue).endObject()
                            .endObject()
                        .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();
    }

    /**
     * Create a river and optionaly parameter it with a percolator mode
     *
     * @param percolateMode used to name the river
     * @param ordered define if the river will process bulk request in an ordered manner and used to name river too
     * @throws IOException
     */
    private static void createRiver(String exchangeQueue, RabbitmqRiver.PercolateMode percolateMode, boolean ordered) throws IOException {
        String riverName = (percolateMode != null) ? "rabbitmq-" + percolateMode + "-" + ordered: "rabbitmq-default-" + ordered;
        XContentBuilder riverBuilder = jsonBuilder().startObject().field("type", "rabbitmq");
        if (percolateMode != null) {
                riverBuilder
                        .startObject("rabbitmq")
                            .field("exchange", exchangeQueue)
                            .field("queue", exchangeQueue)
                            .field("routing_key", exchangeQueue)
                        .endObject()
                        .startObject("index").field("ordered", ordered).endObject()
                        .startObject("percolate").field("percolate_mode", percolateMode).endObject();
        }
        riverBuilder.endObject();
        client.prepareIndex("_river", riverName, "_meta").setSource(riverBuilder).execute().actionGet();
    }

    @AfterClass
    public static void tearDown() {
        node.close();
    }

}
