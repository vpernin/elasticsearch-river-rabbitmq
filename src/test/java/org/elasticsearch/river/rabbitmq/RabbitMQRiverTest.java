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
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class RabbitMQRiverTest {

    public static void main(String[] args) throws Exception {

        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();

        // Clean
        node.client().admin().indices().prepareDelete(new String[]{}).execute().actionGet();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder()
                .startObject()
                .field("type", "rabbitmq")
                .startObject("percolate").field("enabled", "true").endObject()
                .endObject()).execute().actionGet();

        // Register two percolators
        node.client()
                .prepareIndex("_percolator", "test", "percolator1")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term")
                        .field("field1", "value1")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();

        node.client()
                .prepareIndex("_percolator", "test", "percolator2")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term")
                        .field("field2", "value2")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject().field("type", "rabbitmq").endObject()).execute().actionGet();

        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setHost("localhost");
        cfconn.setPort(AMQP.PROTOCOL.PORT);
        Connection conn = cfconn.newConnection();

        Channel ch = conn.createChannel();
        ch.exchangeDeclare("elasticsearch", "direct", true);
        ch.queueDeclare("elasticsearch", true, false, false, null);


        // Standard request
        String message = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
                "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }";

        // Percolated requests
        String percolateMessageOneMatch = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\", \"_percolate\" : \"*\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n";

        String percolatedMessageTwoMatches = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"4\", \"_percolate\" : \"*\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\", \"field2\" : \"value2\" } }\n";

        String percolateMessageWrongQuery = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"5\", \"_percolate\" : \"doesNotExist\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n";

        long publishDate = System.currentTimeMillis();

        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, percolateMessageOneMatch.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, percolatedMessageTwoMatches.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, percolateMessageWrongQuery.getBytes());

        ch.close();
        conn.close();

        Thread.sleep(5000);

        // Check if the default index holding percolator match contains our request
        SearchResponse searchResponse = node.client().prepareSearch("percolate_match")
                .setQuery(matchAllQuery())
                .addSort("percolate_id", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().hits().length, equalTo(3));
        assertPercolateMatch(searchResponse.hits().getAt(0), "test", "type1", "1", Arrays.asList("percolator1"), publishDate);
        assertPercolateMatch(searchResponse.hits().getAt(1), "test", "type1", "3", Arrays.asList("percolator1"), publishDate);
        assertPercolateMatch(searchResponse.hits().getAt(2), "test", "type1", "4", Arrays.asList("percolator1", "percolator2"), publishDate);

        System.out.println("Everything is ok");
    }

    private static void assertPercolateMatch(SearchHit hit, String percolateIndex,
                                             String percolateType, String id, List<String> matches, long publishDate) {
        assertThat(hit.type(), equalTo("percolate"));
        assertThat(hit.getSource().get("percolate_index").toString(), equalTo(percolateIndex));
        assertThat(hit.getSource().get("percolate_type").toString(), equalTo(percolateType));
        assertThat(hit.getSource().get("percolate_id").toString(), equalTo(id));
        Assert.assertTrue(ISODateTimeFormat.dateTime().parseDateTime(hit.getSource().get("percolate_date").toString()).isAfter(publishDate));
        assertThat(hit.getSource().get("percolate_match").toString(), equalTo(matches.toString()));
    }
}
