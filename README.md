# electron
- Launch baikal-devenv through docker-compose (tested against tag v2.6.0.3)
```shell
git clone git@github.com:ComputationalHealth/baikal-devenv.git
cd baikal-devenv/compose
docker-compose up -d --build
```
- Run HDFS startup.sh config script
```shell
docker exec hadoop-namenode /bin/bash startup.sh
```
- Load Jupyter in browser, upload demo files
  - Run ZookeeperConfig.ipynb cells (this creates the node for Storm to store Kafka offsets)
- Copy and load topology
  - Download (https://github.com/ComputationalHealth/electron/releases/download/v1.0.0/electron-1.0.0-shaded.jar) or build shaded electron jar, then:
```shell
docker cp electron-1.0.0-shaded.jar supervisor:/
docker exec supervisor storm jar /electron-1.0.0-shaded.jar org.yale.comphealth.electron.topology.ConsumeSignalTopology
```
- Load Zeppelin (port 9001)
	- Add Avro dependency/artifact to Spark interpreter: com.databricks:spark-avro_2.11:4.0.0
	- Import notebook from analytics/ElectronAnalytics.json
