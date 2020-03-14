/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.ComputePointsBolt;
import stormTP.operator.ComputeScoreBolt;
import stormTP.operator.Exit5Bolt;
import stormTP.operator.ExitBolt;
import stormTP.operator.MasterInputStreamSpout;
import stormTP.operator.test.TestStatefulBolt;

/**
 *
 * @author keraghel
 */
public class TopologyT5 {
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
        builder.setBolt("ComputeScoreBolt", new ComputeScoreBolt(), nbExecutors).shuffleGrouping("ComputePointsBolt");
        builder.setBolt("exit5Bolt", new Exit5Bolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("ComputeScoreBolt");
        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT5", config, builder.createTopology());
    }
}
