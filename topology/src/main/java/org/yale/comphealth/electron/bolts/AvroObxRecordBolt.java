package org.yale.comphealth.electron.bolts;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import com.esotericsoftware.minlog.Log;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yale.comphealth.electron.models.ObxAvroModel;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.Map;

public class AvroObxRecordBolt extends AbstractHdfsBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AvroObxRecordBolt.class);

    private transient FSDataOutputStream out;
    private Schema schema;
    private DataFileWriter<ObxAvroModel> avroWriter;
    private boolean enableSnappyCompression = false;

    public AvroObxRecordBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public AvroObxRecordBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public AvroObxRecordBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public AvroObxRecordBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public AvroObxRecordBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public AvroObxRecordBolt withSnappyCompression(boolean isEnabled){
        this.enableSnappyCompression = isEnabled;
        return this;
    }

    public AvroObxRecordBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public AvroObxRecordBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    @Override
    void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.debug("Preparing AvroGenericRecord Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        this.schema = ObxAvroModel.SCHEMA$;
    }

    @Override
    void writeTuple(Tuple tuple) throws IOException {
        ObxAvroModel avroRecord = (ObxAvroModel) tuple.getValue(0);
        avroWriter.append(avroRecord);
        offset = this.out.getPos();
    }

    @Override
    void syncTuples() throws IOException {
        avroWriter.flush();

        LOG.debug("Attempting to sync all data to filesystem");
        if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
        this.syncPolicy.reset();
    }

    @Override
    protected void closeOutputFile() throws IOException
    {
        avroWriter.close();
        this.out.close();
    }

    @Override
    Path createOutputFile() throws IOException {
        Log.debug("Creating output file...");

        String[] filename = this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()).split("-");

        // Add subpath based on year/month to rotate Avro files
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);

        String monthStr = Integer.toString(month);
        if(month < 10){
            monthStr = "0" + monthStr;
        }

        String dayStr = Integer.toString(day);
        if(day < 10){
            dayStr = "0" + dayStr;
        }

        Path path = new Path(Paths.get(this.fileNameFormat.getPath(), Integer.toString(year),
                monthStr, dayStr, filename[filename.length - 1]).toString());

        this.out = this.fs.create(path);

        //Initialize writer
        DatumWriter<ObxAvroModel> datumWriter = new GenericDatumWriter<>(ObxAvroModel.SCHEMA$);
        avroWriter = new DataFileWriter<>(datumWriter);

        if(enableSnappyCompression) {
            avroWriter.setCodec(CodecFactory.snappyCodec());
        }
        avroWriter.create(this.schema, this.out);

        return path;
    }
}