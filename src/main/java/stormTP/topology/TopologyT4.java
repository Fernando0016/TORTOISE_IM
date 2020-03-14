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
import stormTP.operator.Exit4Bolt;
import stormTP.operator.MasterInputStreamSpout;

/**
 *
 * @author keraghel
 */
public class TopologyT4 {
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
        builder.setBolt("exit4Bolt", new Exit4Bolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("ComputePointsBolt");
        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }
}
