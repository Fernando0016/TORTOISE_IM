/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.topology;

import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.AveragePointsBolt;
import stormTP.operator.ComputePointsBolt;
import stormTP.operator.EvolAvgPointsBolt;
import stormTP.operator.Exit7Bolt;
import stormTP.operator.IntermediaryFilter;
import stormTP.operator.MasterInputStreamSpout;

/**
 *
 * @author keraghel
 */
public class TopologyT7 {

    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = 9001;
        int portOUTPUT = 9002;
        String ipmINPUT = "224.0.0." + args[0];
        String ipmOUTPUT = "225.0." + args[0] + "." + args[1];
        /*Création du spout*/
        MasterInputStreamSpout spout = new MasterInputStreamSpout(portINPUT, ipmINPUT);
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        builder.setBolt("ComputePointsBolt", new ComputePointsBolt(), nbExecutors).shuffleGrouping("masterStream");
        builder.setBolt("IntermediaryFilter", new IntermediaryFilter(), nbExecutors).shuffleGrouping("ComputePointsBolt");
        builder.setBolt("AveragePointsBolt", new AveragePointsBolt()
                .withTumblingWindow(new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS))
                .withMessageIdField("idMsg"), nbExecutors
        ).shuffleGrouping("IntermediaryFilter");
        builder.setBolt("EvolAvgPointsBolt", new EvolAvgPointsBolt()
                .withWindow(new BaseWindowedBolt.Count(2))
                .withMessageIdField("msgId"), nbExecutors
        ).shuffleGrouping("AveragePointsBolt");
        builder.setBolt("exit7Bolt", new Exit7Bolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("EvolAvgPointsBolt");

        Config config = new Config();
        StormSubmitter.submitTopology("topoT7", config, builder.createTopology());
    }
}
