/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.io.StringReader;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
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
public class GiveRankBolt implements IRichBolt{
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
    }

    @Override
    public void execute(Tuple t) {              
        TortoiseManager tm = new TortoiseManager(3,"KERAGHEL-TOUATIOUI");
        
        Runner runner = tm.computeRank(t.getLongByField("id"),
                t.getLongByField("top"),
                t.getStringByField("nom"),
                t.getIntegerByField("nbDevant"),
                t.getIntegerByField("nbDerriere"),
                t.getIntegerByField("total")
        );
	Values v = new Values();
        v.add(runner.getId());
        v.add(runner.getTop());
        v.add(runner.getNom());
        v.add(runner.getRang());
        v.add(runner.getTotal());
        collector.emit(t,v);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("id", "top", "nom", "rang", "total"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
}
