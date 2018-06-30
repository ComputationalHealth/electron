package org.yale.comphealth.electron.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.yale.comphealth.electron.models.ObxAvroModel;
import org.yale.comphealth.electron.models.ObxModel;
import org.yale.comphealth.electron.models.SignalModel;

import java.util.*;

/**
 * Created by Wade Schulz on 12/23/2015.
 */
public class ObxSplitterBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector){
        _collector = collector;
    }

    @Override
    public void execute(Tuple input){
        String json = "";
        GsonBuilder builder = new GsonBuilder();
        builder.setDateFormat("yyyy-MM-dd'T'HH:mm:ssz");
        Gson gson = builder.create();

        List<ObxAvroModel> results = new ArrayList<>();
        int maxLine = 0;

        try {
            json = input.getString(0);
            SignalModel signals = gson.fromJson(json, SignalModel.class);

            for (ObxModel obx : signals.ObxObjects) {

                ObxAvroModel result = new ObxAvroModel();
                result.setMshTs(signals.MshTimestamp.getTime()/1000);
                result.setAlarmTs(signals.StartTimestamp.getTime()/1000);

                if(signals.HelixReceivedTimestamp != null) {
                    result.setHelixRcvTs(signals.HelixReceivedTimestamp.getTime() / 1000);
                }else {
                    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    result.setHelixRcvTs(cal.getTimeInMillis() / 1000);
                }

                if(signals.MessageId != null) {
                    result.setMsgId(signals.MessageId);
                }else{
                    result.setMsgId("unknown");
                }

                result.setTzOffset(signals.TimezoneOffset);

                result.setSource(signals.Source);
                result.setUnit(signals.Unit);
                if(signals.Bed != null) {
                    result.setBed(signals.Bed);
                }else{
                    result.setBed("unknown");
                }
                result.setChannel(obx.Channel);
                result.setText(obx.Text);

                if(obx.LineCounter != null) {
                    int line = Integer.parseInt(obx.LineCounter);
                    result.setMsgLine(line);
                    if(line > maxLine){
                        maxLine = line;
                    }
                }

                results.add(result);
            }

            for(ObxAvroModel result : results){
                result.setMsgMaxLine(maxLine);
                _collector.emit(new Values(result));
            }

            _collector.ack(input);
        }catch(IllegalStateException | JsonSyntaxException exception){
            Log.error(json);
            Log.error(exception.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obx"));
    }
}
