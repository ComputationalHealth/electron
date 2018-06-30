package org.yale.comphealth.electron.bolts;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractHdfsBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsBolt.class);
    private static final Integer DEFAULT_RETRY_COUNT = 3;

    protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
    private Path currentFile;
    protected OutputCollector collector;
    protected transient FileSystem fs;
    protected SyncPolicy syncPolicy;
    protected FileRotationPolicy rotationPolicy;
    protected FileNameFormat fileNameFormat;
    protected int rotation = 0;
    protected String fsUrl;
    protected String configKey;
    protected transient Object writeLock;
    protected transient Timer rotationTimer; // only used for TimedRotationPolicy
    private List<Tuple> tupleBatch = new LinkedList<>();
    protected long offset = 0;
    protected Integer fileRetryCount = DEFAULT_RETRY_COUNT;
    protected Integer tickTupleInterval = 0;

    protected transient Configuration hdfsConfig;

    protected void rotateOutputFile() throws IOException {
        LOG.debug("Rotating output file...");
        long start = System.currentTimeMillis();
        synchronized (this.writeLock) {
            closeOutputFile();
            this.rotation++;

            Path newFile = createOutputFile();
            LOG.debug("Performing {} file rotation actions.", this.rotationActions.size());
            for (RotationAction action : this.rotationActions) {
                action.execute(this.fs, this.currentFile);
            }
            this.currentFile = newFile;
        }
        long time = System.currentTimeMillis() - start;
        LOG.debug("File rotation took {} ms.", time);
    }

    /**
     * Marked as final to prevent override. Subclasses should implement the doPrepare() method.
     * @param conf
     * @param topologyContext
     * @param collector
     */
    public final void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector){
        this.writeLock = new Object();
        if (this.syncPolicy == null) throw new IllegalStateException("SyncPolicy must be specified.");
        if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
        if (this.fsUrl == null) {
            throw new IllegalStateException("File system URL must be specified.");
        }

        this.collector = collector;
        this.fileNameFormat.prepare(conf, topologyContext);
        this.hdfsConfig = new Configuration();
        Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
        if(map != null){
            for(String key : map.keySet()){
                this.hdfsConfig.set(key, String.valueOf(map.get(key)));
            }
        }

        // If interval is non-zero then it has already been explicitly set and we should not default it
        if (conf.containsKey("topology.message.timeout.secs") && tickTupleInterval == 0)
        {
            Integer topologyTimeout = Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
            tickTupleInterval = (int)(Math.floor(topologyTimeout / 2));
            LOG.debug("Setting tick tuple interval to [{}] based on topology timeout", tickTupleInterval);
        }

        try{
            HdfsSecurityUtil.login(conf, hdfsConfig);
            doPrepare(conf, topologyContext, collector);
            this.currentFile = createOutputFile();
            Log.debug("Filename: " + this.currentFile.getName());
        } catch (Exception e){
            throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
        }

        if(this.rotationPolicy instanceof TimedRotationPolicy){
            long interval = ((TimedRotationPolicy)this.rotationPolicy).getInterval();
            this.rotationTimer = new Timer(true);
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        Log.debug("Timed file rotation executing...");
                        rotateOutputFile();
                    } catch(IOException e){
                        LOG.warn("IOException during scheduled file rotation.", e);
                    }
                }
            };
            this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
        }
    }

    @Override
    public final void execute(Tuple tuple) {

        synchronized (this.writeLock) {
            boolean forceSync = false;
            if (isTick(tuple)) {
                LOG.debug("TICK! forcing a file system flush");
                forceSync = true;
            } else {
                try {
                    writeTuple(tuple);
                    tupleBatch.add(tuple);
                } catch (IOException e) {
                    //If the write failed, try to sync anything already written
                    LOG.info("Tuple failed to write, forcing a flush of existing data.");
                    this.collector.reportError(e);
                    forceSync = true;
                    this.collector.fail(tuple);
                }
            }

            if (this.syncPolicy.mark(tuple, this.offset) || (forceSync && tupleBatch.size() > 0)) {
                int attempts = 0;
                boolean success = false;
                IOException lastException = null;
                // Make every attempt to sync the data we have.  If it can't be done then kill the bolt with
                // a runtime exception.  The filesystem is presumably in a very bad state.
                while (success == false && attempts < fileRetryCount) {
                    attempts += 1;
                    try {
                        syncTuples();
                        LOG.debug("Data synced to filesystem. Ack'ing [{}] tuples", tupleBatch.size());
                        for (Tuple t : tupleBatch) {
                            this.collector.ack(t);
                        }
                        tupleBatch.clear();
                        syncPolicy.reset();
                        success = true;
                    } catch (IOException e) {
                        LOG.warn("Data could not be synced to filesystem on attempt [{}]", attempts);
                        this.collector.reportError(e);
                        lastException = e;
                    }
                }

                // If unsuccesful fail the pending tuples
                if (success == false) {
                    LOG.warn("Data could not be synced to filesystem, failing this batch of tuples");
                    for (Tuple t : tupleBatch) {
                        this.collector.fail(t);
                    }
                    tupleBatch.clear();

                    throw new RuntimeException("Sync failed [" + attempts + "] times.", lastException);
                }
            }

            if(this.rotationPolicy.mark(tuple, this.offset)) {
                try {
                    rotateOutputFile();
                    this.rotationPolicy.reset();
                    this.offset = 0;
                } catch (IOException e) {
                    this.collector.reportError(e);
                    LOG.warn("File could not be rotated");
                    //At this point there is nothing to do.  In all likelihood any filesystem operations will fail.
                    //The next tuple will almost certainly fail to write and/or sync, which force a rotation.  That
                    //will give rotateAndReset() a chance to work which includes creating a fresh file handle.
                }
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = super.getComponentConfiguration();
        if (conf == null)
            conf = new Config();

        if (tickTupleInterval > 0) {
            LOG.info("Enabling tick tuple with interval [{}]", tickTupleInterval);
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTupleInterval);
        }

        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * writes a tuple to the underlying filesystem but makes no guarantees about syncing data.
     *
     * this.offset is also updated to reflect additional data written
     *
     * @param tuple
     * @throws IOException
     */
    abstract void writeTuple(Tuple tuple) throws IOException;

    /**
     * Make the best effort to sync written data to the underlying file system.  Concrete classes should very clearly
     * state the file state that sync guarantees.  For example, HdfsBolt can make a much stronger guarantee than
     * SequenceFileBolt.
     *
     * @throws IOException
     */
    abstract void syncTuples() throws IOException;

    abstract void closeOutputFile() throws IOException;

    abstract Path createOutputFile() throws IOException;

    abstract void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException;


    private boolean isTick(Tuple tuple) {
        return tuple != null
                && Constants.SYSTEM_COMPONENT_ID  .equals(tuple.getSourceComponent())
                && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }
}