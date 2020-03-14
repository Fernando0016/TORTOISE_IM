/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stormTP.operator;

import java.util.Map;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import stormTP.core.TortoiseManager;

/**
 *
 * @author sarah
 */
public class EvolAvgPointsBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
    private static final long serialVersionUID = 4262379330788107343L;
    private KeyValueState<String, Integer> kvState;
    private OutputCollector collector;
    int cpt;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void initState(KeyValueState<String, Integer> state) {
        this.kvState = state;
        this.cpt = kvState.get("cpt", 0);
    }
    
    @Override
    public void execute(TupleWindow inputWindow) {
        if(inputWindow.get().size() == 2) {
            double avg1 = inputWindow.get().get(0)
                    .getDoubleByField("average");
            double avg2 = inputWindow.get().get(1)
                    .getDoubleByField("average");
            
            TortoiseManager tm = new TortoiseManager();
            String evolution = tm.giveRankEvolution(avg1, avg2);

            this.cpt++;
            kvState.put("cpt", this.cpt);
            
            Values values = new Values();
            values.add(tm.getDossard());
            values.add(this.cpt);
            values.add(evolution);
            collector.emit(inputWindow.get(), values);
        }                                
    }


    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "cpt", "evolution"));
    }
    
}


