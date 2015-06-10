package storm.starter;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Harini Rajendran on 6/7/15.
 */

public class TestSlidingWindow extends BaseWindowBolt{
    final static Logger LOG = Logger.getLogger(TumblingWindow.class.getName());
    OutputCollector _collector;
    long windowStart;
    long windowEnd;
    long tupleCount;

    public TestSlidingWindow(WindowObject wObj)
    {
        super(wObj.getWindowLength(), wObj.getSlideBy(), wObj.getIsTimeBased(), -1);
        windowStart = 1;
        windowEnd = wObj.getWindowLength();
        tupleCount = 0;
        if(isTimeBased())
        {
            System.out.println("Window Start::" + tupleCount);
            addStartAddress(0l);
            windowStart += getSlideByValue();
        }
    }

    @Override
    public final void execute(Tuple tuple) {
        if(isTimeBased())
        {
            if (isTickTuple(tuple)) {
                System.out.println("Count for this second::" + secondCount);//Testing
                secondCount = 0;//Testing
                tupleCount++;
                if(tupleCount == windowStart-1)
                {
                    System.out.println("Window Start::" + tupleCount);
                    storeTuple(tuple, 0, 1);
                    windowStart += getSlideByValue();
                }
                if (tupleCount == windowEnd) {
                    System.out.println("Window End::" + (tupleCount));
                    storeTuple(tuple, 1, 1);
                    windowEnd += getSlideByValue();
                }
            }
            else {
                secondCount++;//Testing
                storeTuple(tuple, -1, 1);
            }
        }
        else {
            tupleCount++;
            if (tupleCount == windowStart) {
                System.out.println("Window Start::" + tupleCount);
                storeTuple(tuple, 0, 1);
                windowStart += getSlideByValue();
            }
            if (tupleCount == windowEnd) {
                System.out.println("Window End::" + (tupleCount));
                storeTuple(tuple, 1, 1);
                windowEnd += getSlideByValue();
            } else if(tupleCount != windowStart)
                storeTuple(tuple, -1, 1);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("RandomInt"));
        declarer.declareStream("mockTickTuple", new Fields("MockTick"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,1);
        return conf;
    }

    @Override
    public void cleanup(){
        readFromFile();
    }

}
