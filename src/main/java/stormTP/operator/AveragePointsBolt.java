package stormTP.operator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.UUID;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import stormTP.core.TortoiseManager;

public class AveragePointsBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {

    private static final long serialVersionUID = 4262379330788107343L;
    private KeyValueState<String, Integer> state;
    private double average;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        this.state = state;
        average = 0;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int nbTuples = inputWindow.get().size();
        
        // get id
        TortoiseManager tm = new TortoiseManager();

        // compoute avg
        int sum = 0;
        for (Tuple t : inputWindow.get()) {
            sum += t.getIntegerByField("points");
        }
        average = (double) sum / nbTuples;
        
        // compute tops
        Tuple T0 = inputWindow.get().get(0);
        Tuple Tn = inputWindow.get().get(nbTuples - 1);
        String topsFirstLast = T0.getLongByField("top") + "-" + Tn.getLongByField("top");
        
        Values values = new Values();
        values.add(tm.getDossard());
        values.add(topsFirstLast);
        values.add(average);
        values.add(new Random().nextLong());
        
        collector.emit(inputWindow.get(), values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "average", "msgId"));
    }
}
