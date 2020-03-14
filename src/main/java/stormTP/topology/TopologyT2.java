/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit2Bolt;
import stormTP.operator.MasterInputStreamSpout;
import stormTP.operator.MyTortoiseBolt;

/**
 *
 * @author keraghel
 */
public class TopologyT2 {
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
    /*Affectation à la topologie du bolt qui ne fait rien, il prendra en input le spout localStream*/
    builder.setBolt("MyTortoiseBolt", new MyTortoiseBolt(), nbExecutors).shuffleGrouping("masterStream");
    /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
    builder.setBolt("exit2Bolt", new Exit2Bolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("MyTortoiseBolt");

    /*Création d'une configuration*/
    Config config = new Config();
    /*La topologie est soumise à STORM*/
    StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
    }
}
