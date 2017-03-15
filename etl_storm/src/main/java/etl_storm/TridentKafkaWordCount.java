/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */
package etl_storm;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;

import java.util.Properties;

public class TridentKafkaWordCount {

    private String zkUrl;
    private String brokerUrl;

    TridentKafkaWordCount(String zkUrl, String brokerUrl) {
        this.zkUrl = zkUrl;
        this.brokerUrl = brokerUrl;
    }

    private TransactionalTridentKafkaSpout createKafkaSpout() {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "testin");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        //config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return new TransactionalTridentKafkaSpout(config);
    }


    private Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc) {
        return tridentTopology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"));
    }

    private TridentState addTridentState(TridentTopology tridentTopology) {
        return tridentTopology.newStream("spout1", createKafkaSpout()).parallelismHint(1)
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);
    }

    /**
     * Creates a trident topology that consumes sentences from the kafka "test" topic using a
     * {@link TransactionalTridentKafkaSpout} computes the word count and stores it in a {@link MemoryMapState}.
     * A DRPC stream is then created to query the word counts.
     * @param drpc
     * @return
     */
    public StormTopology buildConsumerTopology(LocalDRPC drpc) {
        TridentTopology tridentTopology = new TridentTopology();
        addDRPCStream(tridentTopology, addTridentState(tridentTopology), drpc);
        return tridentTopology.build();
    }

    /**
     * Return the consumer topology config.
     *
     * @return the topology config
     */
    public Config getConsumerConfig() {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        //  conf.setDebug(true);
        return conf;
    }

    /**
     * A topology that produces random sentences using {@link RandomSentenceSpout} and
     * publishes the sentences using a KafkaBolt to kafka "test" topic.
     *
     * @return the storm topology
     */
    public StormTopology buildProducerTopology(Properties prop) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 2);

        KafkaBolt bolt = new KafkaBolt().withProducerProperties(prop)
                .withTopicSelector(new DefaultTopicSelector("testout"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "word"));
        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");
        return builder.createTopology();
    }

    public Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "storm-kafka-producer");
        return props;
    }

    public static void main(String[] args) throws Exception {

        String zkUrl = "localhost:2181";
        String brokerUrl = "localhost:9092";

        TridentKafkaWordCount wordCount = new TridentKafkaWordCount(zkUrl, brokerUrl);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("wordCounter", wordCount.getConsumerConfig(), wordCount.buildConsumerTopology(drpc));

        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        cluster.submitTopology("kafkaBolt", conf, wordCount.buildProducerTopology(wordCount.getProducerConfig()));

        for (int i = 0; i < 5; i++) {
            System.out.println(">>>DRPC RESULT: " + drpc.execute("words", "1034 FI_RT FI"));
            Thread.sleep(1000);
        }

        cluster.killTopology("kafkaBolt");
        cluster.killTopology("wordCounter");
        cluster.shutdown();
    }
}
