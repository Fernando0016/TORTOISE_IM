/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.math.BigDecimal;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.core.Runner;
import stormTP.stream.StreamEmiter;

/**
 *
 * @author sarah
 */
public class Exit2Bolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107342L;
    //private static Logger logger = Logger.getLogger("ExitBolt");
    private OutputCollector collector;
    String ipM = "";
    int port = -1;
    StreamEmiter semit = null;

    public Exit2Bolt (int port, String ip) {
            this.port = port;
            this.ipM = ip; 
            this.semit = new StreamEmiter(this.port,this.ipM);

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    @Override
    public void execute(Tuple t) {

        Runner runner = new Runner(t.getLongByField("id"),
                t.getStringByField("nom"),
                t.getIntegerByField("nbDevant"),
                t.getIntegerByField("nbDerriere"),
                t.getIntegerByField("total"),
                t.getIntegerByField("position"),
                t.getLongByField("top")
        ); 

        this.semit.send(runner.getJSON_V1());
        collector.ack(t);
    }



    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
            arg0.declare(new Fields("json"));
    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
            return null;
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#cleanup()
     */
    public void cleanup() {

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
    }
}
