
package org.yale.comphealth.electron.topology;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.minlog.Log;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.w3c.dom.Document;
import org.yale.comphealth.electron.bolts.AvroObrRecordBolt;
import org.yale.comphealth.electron.bolts.AvroObxRecordBolt;
import org.yale.comphealth.electron.bolts.ObrProcessorBolt;
import org.yale.comphealth.electron.bolts.ObxSplitterBolt;
import org.yale.comphealth.electron.models.ObxAvroModel;
import org.yale.comphealth.electron.models.ObrAvroModel;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Wade Schulz on 10/22/2015.
 * Updated by Wade Schulz on 5/4/2016-5/6/2016
 * Updated by Wade Schulz on 11/4/2016-11/5/2016
 * Updated by Wade Schulz on 9/2017
 */
public class ConsumeSignalTopology {
    private static final String SIGNAL_SPOUT_ID = "kafka-signal-spout";
    private static final String OBR_PROC_BOLT = "obr-proc-bolt";
    private static final String OBR_HDFS_BOLT = "obr-hdfs-bolt";
    private static final String OBX_DENORM_BOLT = "obx-denorm-bolt";
    private static final String HDFS_BOLT_ID = "hdfs-avro-bolt";
    private static final String TOPOLOGY_NAME = "electron-topology";

    public static void main(String[] args) throws Exception {
        Log.info("Starting {} version " + getVersion(), TOPOLOGY_NAME);

        int numSpoutExecutors = 1; // should match kafka partition #

        KafkaSpout kafkaSpout = buildKafkaSpout();

        ObxSplitterBolt obxDenormalizationBolt = new ObxSplitterBolt();
        ObrProcessorBolt obrProcessorBolt = new ObrProcessorBolt();

        // sync the filesystem after every X tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(25);

        // rotate files when they reach configured size
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(128.0f, FileSizeRotationPolicy.Units.MB);

        // set base path -- AvroObxRecordBolt will append year and month
        FileNameFormat obxFileNameFormat = new DefaultFileNameFormat()
                .withPath("/data/electron/signals/avro/")
                .withExtension(".avro");

        // Set to namespace for HA HDFS, otherwise set to primary namenode
        AvroObxRecordBolt obxBolt = new AvroObxRecordBolt()
                .withFsUrl("hdfs://hadoop-namenode")
                .withConfigKey("hdfs.config")
                .withFileNameFormat(obxFileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withSnappyCompression(false); // can turn on snappy if desired

        // set base path -- AvroObxRecordBolt will append year and month
        FileNameFormat obrFileNameFormat = new DefaultFileNameFormat()
                .withPath("/data/electron/raw/avro/")
                .withExtension(".avro");

        // Set to namespace for HA HDFS, otherwise set to primary namenode
        AvroObrRecordBolt obrBolt = new AvroObrRecordBolt()
                .withFsUrl("hdfs://hadoop-namenode")
                .withConfigKey("hdfs.config")
                .withFileNameFormat(obrFileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withSnappyCompression(false); // can turn on snappy if desired

        TopologyBuilder builder = new TopologyBuilder();

        // Read from spout, deserialize and denormalize, write to HDFS as compressed avro
        builder.setSpout(SIGNAL_SPOUT_ID, kafkaSpout, numSpoutExecutors);
        builder.setBolt(OBX_DENORM_BOLT, obxDenormalizationBolt, 1).shuffleGrouping(SIGNAL_SPOUT_ID).setNumTasks(1);
        builder.setBolt(HDFS_BOLT_ID, obxBolt).shuffleGrouping(OBX_DENORM_BOLT);

        builder.setBolt(OBR_PROC_BOLT, obrProcessorBolt, 1).shuffleGrouping(SIGNAL_SPOUT_ID).setNumTasks(1);
        builder.setBolt(OBR_HDFS_BOLT, obrBolt, 1).shuffleGrouping(OBR_PROC_BOLT).setNumTasks(1);

        // Register Kryo serializer
        Config cfg = new Config();
        cfg.registerSerialization(ObxAvroModel.class, FieldSerializer.class);
        cfg.registerSerialization(ObrAvroModel.class, FieldSerializer.class);

        /*
          For Kerberized clusters, uncomment and add the following section
         */
        /*
        cfg.setDebug(false);

        Map<String, Object> map = new HashMap<String,Object>();
        map.put("hdfs.keytab.file","/etc/security/keytabs/stormtopology.keytab");
        map.put("hdfs.kerberos.principal","stormtopology");

        cfg.put(HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY, "/etc/security/keytabs/stormtopology.keytab");
        cfg.put(HdfsSecurityUtil.STORM_USER_NAME_KEY, "stormtopology");
        cfg.put("hdfs.config", map);
         */

        // Submit topology to cluster
        StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
    }

    private static KafkaSpout buildKafkaSpout() {
        String zkHostPort = "zookeeper:2181";
        String topic = "electron";

        // zkRoot is where the kafka offset will be stored
        String zkRoot = "/" + topic;

        // zkSpoutId should be a unique key for the spout
        // UUID.randomUUID will generate a new spout id for each build
        // Setting to a specific value will allow pickup from the same kafka offset
        //String zkSpoutId = UUID.randomUUID().toString();
        String zkSpoutId = "5abc7363-8327-4eac-820a-9b1a5b4fcaa5";

        ZkHosts zkHosts = new ZkHosts(zkHostPort);

        SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
        spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);

        return kafkaSpout;
    }

    public synchronized static final String getVersion() {
        // Try to get version number from pom.xml (available in Eclipse)
        try {
            String className = ConsumeSignalTopology.class.getName();
            String classfileName = "/" + className.replace('.', '/') + ".class";
            URL classfileResource = ConsumeSignalTopology.class.getResource(classfileName);
            if (classfileResource != null) {
                Path absolutePackagePath = Paths.get(classfileResource.toURI())
                        .getParent();
                int packagePathSegments = className.length()
                        - className.replace(".", "").length();
                // Remove package segments from path, plus two more levels
                // for "target/classes", which is the standard location for
                // classes in Eclipse.
                Path path = absolutePackagePath;
                for (int i = 0, segmentsToRemove = packagePathSegments + 2;
                     i < segmentsToRemove; i++) {
                    path = path.getParent();
                }
                Path pom = path.resolve("pom.xml");
                try (InputStream is = Files.newInputStream(pom)) {
                    Document doc = DocumentBuilderFactory.newInstance()
                            .newDocumentBuilder().parse(is);
                    doc.getDocumentElement().normalize();
                    String version = (String) XPathFactory.newInstance()
                            .newXPath().compile("/project/version")
                            .evaluate(doc, XPathConstants.STRING);
                    if (version != null) {
                        version = version.trim();
                        if (!version.isEmpty()) {
                            return version;
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Ignore
        }

        // Fallback to using Java API to get version from MANIFEST.MF
        String version = null;
        Package pkg = ConsumeSignalTopology.class.getPackage();
        if (pkg != null) {
            version = pkg.getImplementationVersion();
            if (version == null) {
                version = pkg.getSpecificationVersion();
            }
        }
        version = version == null ? "" : version.trim();
        return version.isEmpty() ? "unknown" : version;
    }
}
