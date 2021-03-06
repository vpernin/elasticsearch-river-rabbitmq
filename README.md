RabbitMQ River Plugin for ElasticSearch
==================================

The RabbitMQ River plugin allows index bulk format messages into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-rabbitmq/1.4.0`.

    --------------------------------------------------------
    | RabbitMQ Plugin | ElasticSearch    | RabbitMQ Client |
    --------------------------------------------------------
    | master          | 0.20.1 -> master | 3.0.1           |
    --------------------------------------------------------
    | 1.4.0           | 0.19 -> master   | 2.8.4           |
    --------------------------------------------------------
    | 1.3.0           | 0.19 -> master   | 2.8.2           |
    --------------------------------------------------------
    | 1.2.0           | 0.19 -> master   | 2.8.1           |
    --------------------------------------------------------
    | 1.1.0           | 0.19 -> master   | 2.7.0           |
    --------------------------------------------------------
    | 1.0.0           | 0.18             | 2.7.0           |
    --------------------------------------------------------

RabbitMQ River allows to automatically index a [RabbitMQ](http://www.rabbitmq.com/) queue and percolate request. The format of the messages follows the bulk api format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

Creating the rabbitmq river is as simple as (all configuration parameters are provided, with default values):

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "rabbitmq",
	    "rabbitmq" : {
	        "host" : "localhost", 
	        "port" : 5672,
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        "queue" : "elasticsearch",
	        "exchange" : "elasticsearch",
	        "routing_key" : "elasticsearch",
	        "exchange_type" : "direct",
	        "exchange_durable" : true,
	        "queue_durable" : true,
	        "queue_auto_delete" : false
	    },
	    "percolate" : {
	        "percolate_mode" : "INDEX_THEN_PERCOLATE | ON_THE_FLY",
	        "percolate_query" : "*",
	        "percolate_index" : "percolate_index",
	        "percolate_match_index" : "percolate_match"
	        "percolate_match_type" : "percolate"
	    },
	    "index" : {
	        "bulk_size" : 100,
	        "bulk_timeout" : "10ms",
	        "ordered" : false
	    }
	}'

If the percolate_mode parameter is set to ON_THE_FLY and the bulk actions do not contain a percolate value, percolate on percolateQuery will be added to each bulk request.
If the percolate_mode parameter is set to INDEX_THEN_PERCOLATE, each bulk request will be percolated against the index percolate_index.
In both cases, the percolate matches will be added to the index with percolate_match_type and percolate_match_type parameters.

Addresses(host-port pairs) also available. it is useful to taking advantage rabbitmq HA(active/active) without any rabbitmq load balancer.
(http://www.rabbitmq.com/ha.html)
	
		...
	    "rabbitmq" : {
	    	"addresses" : [
	        	{
	        		"host" : "rabbitmq-host1", 
	        		"port" : 5672
	        	},
	        	{
	        		"host" : "rabbitmq-host2", 
	        		"port" : 5672
	        	}
	        ],
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        ...
		}
		...

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed. It can also be used as a simple way to throttle indexing.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
