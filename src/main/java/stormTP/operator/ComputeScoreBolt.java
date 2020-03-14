package stormTP.operator;

import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.core.Runner;

public class ComputeScoreBolt extends BaseStatefulBolt<KeyValueState<String, Integer>> {

    private static final long serialVersionUID = 4262379330722107343L;
    KeyValueState<String, Integer> kvState;
    int score;
    private OutputCollector collector;

    @Override
    public void execute(Tuple t) {
        if (kvState.get(t.getLongByField("id").toString()) != null) {
            score = t.getIntegerByField("points") + kvState.get(t.getLongByField("id").toString());
        } else {
            score = t.getIntegerByField("points");
        }
        kvState.put(t.getLongByField("id").toString(), score);
        Values values = new Values();
        values.add(t.getLongByField("id"));
        values.add(t.getLongByField("top"));
        values.add(score);
        values.add(t.getIntegerByField("points"));
        collector.emit(t, values);
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        kvState = state;
        score = kvState.get("score", 0);

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "score", "points"));
    }

}
