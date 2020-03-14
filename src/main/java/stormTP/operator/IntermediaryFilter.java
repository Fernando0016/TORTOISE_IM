/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.util.Map;
import java.util.Random;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.core.TortoiseManager;

/**
 *
 * @author keraghel
 */
public class IntermediaryFilter implements IRichBolt{
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        
        TortoiseManager tm = new TortoiseManager();
        if(tuple.getLongByField("id") == tm.getDossard()){
            Values values = new Values();
            values.add(tuple.getLongByField("id"));
            values.add(tuple.getLongByField("top"));
            values.add(tuple.getIntegerByField("points"));
            Long Msgid = new Random().nextLong();
            values.add(Msgid);
            collector.emit(tuple, values);
        }
    }

    @Override
    public void cleanup() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("id", "top","points","idMsg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
}
