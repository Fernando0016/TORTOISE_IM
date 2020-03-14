/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.io.StringReader;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
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
public class ComputePointsBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {

        String input = tuple.getValueByField("json").toString();
        TortoiseManager tm = new TortoiseManager(3, "KERAGHEL-TOUATIOUI");
        Runner runner = null;
        int bonus = 0;
        JsonReader jsonInput = Json.createReader(new StringReader(input));
        JsonObject jsonObj = jsonInput.readObject();
        jsonInput.close();
        JsonArray tortoises = jsonObj.getJsonArray("tortoises");
        if (tortoises.getJsonObject(0).getInt("top") % 20 == 0) {
            for (int i = 0; i < tortoises.size(); i++) {
                JsonObject tortoise = tortoises.getJsonObject(i);
                runner = tm.computeRank(tortoise.getInt("id"), tortoise.getInt("top"), "", tortoise.getInt("nbDevant"), tortoise.getInt("nbDerriere"), tortoise.getInt("total"));
                bonus = tm.computePoints(runner.getRang(), runner.getTotal());
                Values values = new Values();
                values.add(runner.getId());
                values.add(runner.getTop());
                values.add(bonus);
                collector.emit(tuple, values);
            }

        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("id", "top","points"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
