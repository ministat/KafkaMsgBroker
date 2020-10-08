java -Dspark.master=local -jar target/MsgSimuKafka-1.0-SNAPSHOT-jar-with-dependencies.jar -a localhost:7051,localhost:7151,localhost:7251 -k 192.168.122.1:9092 -t integrate-kudu -n kafka-kudu -m 2
