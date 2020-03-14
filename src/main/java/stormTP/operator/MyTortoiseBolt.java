/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.core.Runner;
import stormTP.core.TortoiseManager;

/**
 *
 * @author keraghel
 */
public class MyTortoiseBolt implements IRichBolt{
    private OutputCollector collector;
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        String input = tuple.getValueByField("json").toString();

        TortoiseManager tm = new TortoiseManager();
        Runner runner = tm.filter(input); 
        Values v = new Values();
        v.add(runner.getId());
        v.add(runner.getTop());
        v.add(runner.getNom());
        v.add(runner.getPosition());
        v.add(runner.getNbDevant());
        v.add(runner.getNbDerriere());
        v.add(runner.getTotal());
	collector.emit(tuple, v);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("id", "top", "nom", "position", "nbDevant", "nbDerriere", "total"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
