# Stateful stream processing in Python

This repository shows how to implement stateful stream processing of a Kafka
stream in Python. The same logic has been implemented with multiple
technologies, in their respective folders.

You'll need many Python versions or at least many virtual environments to run
all the examples, since the technologies have varying support for different
Python versions.

Here are the versions we tested:

| Tool | Python version |
| -- | -- |
| Kafka | 3.11 |
| Flink | 3.8 |
| Spark | 3.11 |
| Bytewax | 3.10 |

We will be running the stream processing locally, so you don't need existing
infrastructure to try the tools!

We will generate events to a Kafka stream, and process those events with
stateful semantics. The first example we implement is summing the data of each
sensor in a time window of a given length. If you use a long enough window, the
sensor values should sum to approximately the same values.

## How to use this repository

Firstly, you should have a local Kafka instance, since the examples here don't
support authentication to remote clusters. You may spin up a local cluster using
the `docker-compose.yml` file in the `kafka` folder.

```shell
cd kafka
docker compose up -d
```

By default you'll now have a cluster running at `127.0.0.1:9092`.

Next, you need to create a new Kafka topic and start streaming messages to it.
To interact with the Kafka cluster, we provide Python tools in the `kafka`
folder. Spin up a Python virtual environment using your favourite tool, and run

```shell
pip install -r kafka/requirements.txt
python kafka/create_topic.py -a 127.0.0.1:9092 -t iot
python kafka/produce.py -s 3
```

All programs will accept arguments for the Kafka address and topic name, but
they default to `127.0.0.1:9092` and `iot`.

which will create the `iot` topic to your local cluster, and start the event
stream emulating `n` sensors. The sensors produce intereger values from the
Poisson distribution every second, emulating events such as number of
occurences.

Now you are ready to test the different stateful stream processing technologies!

### Bytewax

Make a new virtual environment with Python version corresponding the above
table. Then run

```shell
pip install -r bytewax/requirements.txt
python bytewax/iot.py
```

Which will start statefully processing the `iot` log. You can see more
configuration options with

```shell
python bytewax/iot.py -h
```

Note that we don't need any additional dependencies to run Bytewax locally.

### Flink

Make a new venv with the correct Python version and install PyFlink:

```shell
pip install -r flink/requirements.txt
python flink/iot.py
```

You will need additional dependencies to run Flink locally. These are packaged
as `jar` files. The matching versions are

- flink-connector-kafka-1.16.1.jar
- kafka-clients-3.3.2.jar

which should be given to Flink in code with `env.add_jars()` or placed to the
`pyflink/lib` folder of your virtual environments site-packages. I've used the
latter method here.

### Spark

We'll be using the Spark Structured Streaming tools. Dependencies are installed
with 

```shell
pip install -r flink/requirements.txt
```

This will install PySpark and give you the `spark-submit` and `spark-shell`
tools. To run the stream processor, we need to tell Spark what additional
dependencies are needed. This can be done together with submitting the stream
processing job. Run:

```shell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark/iot.py
```

With Spark, you should be mindful that the
`spark-sql-kafka` package you specify (Spark will do the downloading for you)
matches your Scala and Spark versions.

Also, beware the log :) 
