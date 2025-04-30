# kafkaplay
Initial steps in probing Kafka as a process management platform

## Setup
```
➜  cd setup
➜  docker compose up
[+] Running 3/3
✔ Network setup_default      Created                                                                                                                  0.0s
✔ Container setup-kafka_b-1  Created                                                                                                                  0.1s
✔ Container kafka-ui         Created                                                                                                                  0.0s
Attaching to kafka-ui, kafka_b-1
kafka_b-1  | kafka 11:12:06.86 INFO  ==>
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Welcome to the Bitnami kafka container
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Subscribe to project updates by watching https://github.com/bitnami/containers
kafka_b-1  | kafka 11:12:06.86 INFO  ==> Did you know there are enterprise versions of the Bitnami catalog? For enhanced secure software supply chain features, unlimited pulls from Docker, LTS support, or application customization, see Bitnami Premium or Tanzu Application Catalog. See https://www.arrow.com/globalecs/na/vendors/bitnami/ for more information.
kafka_b-1  | kafka 11:12:06.86 INFO  ==>
kafka_b-1  | kafka 11:12:06.86 INFO  ==> ** Starting Kafka setup **
kafka-ui   | Standard Commons Logging discovery in action with spring-jcl: please remove commons-logging.jar from classpath in order to avoid potential conflicts
kafka-ui   |  _   _ ___    __             _                _          _  __      __ _
kafka-ui   | | | | |_ _|  / _|___ _ _    /_\  _ __ __ _ __| |_  ___  | |/ /__ _ / _| |_____
kafka-ui   | | |_| || |  |  _/ _ | '_|  / _ \| '_ / _` / _| ' \/ -_) | ' </ _` |  _| / / _`|
kafka-ui   |  \___/|___| |_| \___|_|   /_/ \_| .__\__,_\__|_||_\___| |_|\_\__,_|_| |_\_\__,|
kafka-ui   |                                  |_|                                             
...
```

The 'kafka-ui' tries for a while to connect to 'kafka_b-1', before Kafka is up and running: 

```
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Node -1 disconnected.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Connection to node -1 (kafka_b/172.18.0.2:9092) could not be established. Broker may not be available.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Node -1 disconnected.
kafka-ui   [AdminClient clientId=kafka-ui-admin-1746011530-1] Connection to node -1 (kafka_b/172.18.0.2:9092) could not be established. Broker may not be available.
...
```

Eventually connection is made and things calms down: 

```
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka version: 4.0.0 (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka commitId: 985bc99521dd22bb (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO Kafka startTimeMs: 1746011536964 (org.apache.kafka.common.utils.AppInfoParser)
kafka_b-1  | [2025-04-30 11:12:16,965] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```

