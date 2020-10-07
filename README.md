# Spark Streaming and Kudu Integration example


## Run the applications:

### Launch kudu on local docker

### Launch spotify/kafka on local docker

Reference: https://gist.github.com/abacaphiliac/f0553548f9c577214d16290c2e751071

1. Launch kafka server
   KAFKA_HOST=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1)
   ```
   docker pull spotify/kafka
   docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=$KAFKA_HOST --env ADVERTISED_PORT=9092 --name kafka spotify/kafka
   ```
2. Create topic
   ```
   docker exec kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic integrate-kudu
   ```

### Launch this application
1. Build from project root directory:
   ```
   mvn clean package
   ```
2. Create kudu table: kafka-kudu
   ```
   java -Dspark.master=local -jar target/MsgSimuKafka-1.0-SNAPSHOT-jar-with-dependencies.jar -a localhost:7051,localhost:7151,localhost:7251 -k 192.168.122.1:9092 -t integrate-kudu -n kafka-kudu -m 3
   ```
3. Launch producer:
   ```
   java -jar target/MsgSimuKafka-1.0-SNAPSHOT-jar-with-dependencies.jar -a localhost:7051,localhost:7151,localhost:7251 -k 192.168.122.1:9092 -t integrate-kudu -n kafka-kudu
   ```
4. Launch consumer:
   ```
   java -Dspark.master=local -jar target/MsgSimuKafka-1.0-SNAPSHOT-jar-with-dependencies.jar -a localhost:7051,localhost:7151,localhost:7251 -k 192.168.122.1:9092 -t integrate-kudu -n kafka-kudu -m 2
   ```
