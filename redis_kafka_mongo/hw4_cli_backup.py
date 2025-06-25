import os
import csv
import sys
import uuid
import json
import click
import redis
from time import sleep
from bson import json_util
from tqdm import tqdm
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer, TopicPartition, OffsetAndMetadata


@click.group()
def hw4_cli():
    pass

@hw4_cli.command()
@click.option('--db-name', type=str, default="hw4", help="name of database in mongodb (default 'hw4')")
@click.option('--col-name', type=str, default="taxi", help="name of collection in --db-name in mongodb (default 'taxi')")
@click.option('--input-csv', type=str, help="path to input dataset in csv format", default=None)
@click.argument('mongodb_connection_string', type=str)
@click.argument('output_col_name', type=str)
def task_2(db_name, col_name, input_csv, mongodb_connection_string, output_col_name):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name does not exist
    if col_name in db.list_collection_names():
        print(f"Error: collection with name '{col_name}' already exists in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    if input_csv != None:
        if not os.path.exists(input_csv):
             print(f"task_2: {input_csv} No such file or directory", file=sys.stderr)
             sys.exit(1)
    collection = db[col_name]
    with open(input_csv) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in tqdm(reader, ncols=80, desc="Records uploaded"):
            record = row
            tpep_pickup_datetime = datetime.strptime(record["tpep_pickup_datetime"], "%m/%d/%Y %I:%M:%S %p")
            record["tpep_pickup_datetime"] = tpep_pickup_datetime
            tpep_dropoff_datetime = datetime.strptime(record["tpep_dropoff_datetime"], "%m/%d/%Y %I:%M:%S %p")
            record["tpep_dropoff_datetime"] = tpep_dropoff_datetime
            if (tpep_dropoff_datetime - tpep_pickup_datetime).seconds >= 60:
                record["trip_id"] = str(uuid.uuid4())
                collection.insert_one(record)
    print(f"Created collection '{col_name}' in database '{db_name}' with data from '{input_csv}'")
    first_pipeline = [
        {
            "$project":
            {
                "_id": 1,
                "VendorID": 1,
                "trip_id": 1,
                "tpep_pickup_datetime": 1,
                "timestamp": "$tpep_pickup_datetime",
                "PULocationID": 1
            }
        },
        {
            "$sort":
            {
                "timestamp": 1
            }
        },
        {
            "$merge": output_col_name
        }
    ]
    collection.aggregate(first_pipeline)
    print(f"Created first output in collection '{output_col_name}' in database '{db_name}'")
    second_pipeline = [
        {
            "$project":
            {
                "_id": 0,
                "VendorID": 0,
                "tpep_pickup_datetime": 0,
                "PULocationID": 0
            }
        },
        {
            "$addFields":
            {
                "timestamp": "$tpep_dropoff_datetime",
            }
        },
        {
            "$sort":
            {
                "timestamp": 1
            }
        },
        {
            "$merge": output_col_name
        }
    ]
    collection.aggregate(second_pipeline)
    print(f"Created second output in collection '{output_col_name}' in database '{db_name}'")
    out_collection = db[output_col_name]
    out_collection.create_index({"timestamp": 1})
    print(f"Created index on 'timestamp': 1 field in collection '{output_col_name}' in database '{db_name}'")
    


@hw4_cli.command()
@click.option('--db-name', type=str, default="hw4", help="name of database in mongodb (default 'hw4')")
@click.option('--col-name', type=str, default="taxi", help="name of collection in --db-name in mongodb (default 'taxi')")
@click.option('--kafka-topic', type=str, default="hw4")
@click.argument('mongodb_connection_string', type=str)
@click.argument('kafka_bootstrap_server', type=str)
def task_3(db_name, col_name, kafka_topic, mongodb_connection_string, kafka_bootstrap_server):
    # init kafka producer
    print(f"kafka_bootstrap_server = {kafka_bootstrap_server}")
    producer = KafkaProducer(
        security_protocol="PLAINTEXT",
        api_version=(0, 10),
        bootstrap_servers=[kafka_bootstrap_server], 
        value_serializer=lambda v: json_util.dumps(v).encode('utf-8')
        )
    mongo_client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = mongo_client[db_name]
    # get mongo collection
    collection = db[col_name]
    # pipeline for reading events from mongo
    pipeline = [{"$sort": {"timestamp": 1}}]
    # for doc in list(collection.aggregate(pipeline))[:10]:
    for doc in list(collection.aggregate(pipeline)):
        key = doc["trip_id"]
        producer.send(kafka_topic, key=key.encode('utf-8'), value=doc)
        print(f"send event={doc} with key = {key} in topic='{kafka_topic}'")
        # sleep(1)


@hw4_cli.command()
@click.option('--redis-host', type=str, default="localhost", help="redis host (default 'localhost')")
@click.option('--redis-port', type=int, default=6379, help="redis port (default 6379)")
@click.option('--redis-db', type=int, default=0, help="redis db number (default 0)")
@click.option('--kafka-topic', type=str, default="hw4")
@click.argument('kafka_bootstrap_server', type=str)
def task_4_5(redis_host, redis_port, redis_db, kafka_topic, kafka_bootstrap_server):
    print("start task_4 cli")
    consumer = KafkaConsumer(
        kafka_topic,
        security_protocol="PLAINTEXT",
        api_version=(0, 9),
        bootstrap_servers=[kafka_bootstrap_server],
        # group_id='hw4_consumer_group',
        auto_offset_reset='earliest',
        auto_commit_interval_ms=100,
        enable_auto_commit=True
        )
    tp = TopicPartition(kafka_topic, 1)
    committed_offset = consumer.committed(tp)
    print(f"Partition: {1}, Committed Offset: {committed_offset}")
    print("init kafka consumer")
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    print("init redis client")
    total_active_trips_key = "total_active_trips_count"
    trip_id_pulocation_hs_key = "trip_id_pulocation_map"
    print(f"start kafka loop")
    for msg in consumer:
        print(msg)
        # print(type(msg.value))
        msg_dict = json.loads(msg.value.decode('utf-8'))
        print(f">>>>>>>>>>>>processing kafka event from topic '{kafka_topic}'")
        print(msg_dict)
        with r.pipeline() as pipe:
            if msg_dict.get("tpep_pickup_datetime") != None:
                # Repeat until successful.
                while True:
                    try:
                        pu_trip_id = msg_dict['trip_id']
                        PULocationID = msg_dict['PULocationID']
                        if type(PULocationID) == bytes:
                            PULocationID = PULocationID.decode('utf-8')
                        pulocation_active_trips_count_key = f"trip_id:{pu_trip_id}:PULocationID:{PULocationID}:active_count"
                        # Watch the key we are about to change.
                        pipe.watch(pulocation_active_trips_count_key, total_active_trips_key, trip_id_pulocation_hs_key)
                        # starting redis transcation
                        pipe.multi()
                        print(f">>>>>>>>>>>>starting redis transaction for pickup data update")
                        # incr total active trips count
                        pipe.incr(total_active_trips_key, 1)
                        # update hset
                        pipe.hset(trip_id_pulocation_hs_key, msg_dict["trip_id"], msg_dict["PULocationID"])
                        # incr active trips pulocation
                        pipe.incr(pulocation_active_trips_count_key, 1)
                        pipe.execute()
                        print(f">>>>>>>>>>>>successfully finished redis transaction for pickup data update")
                        break
                    except redis.WatchError:
                        # The transaction failed, so continue with the next attempt.
                        print(f">>>>>>>>>>>>redis transaction for pickup data update failed, retrying...")
                        continue
            else:
                # Repeat until successful.
                while True:
                    try:
                        trip_id = msg_dict["trip_id"]
                        pulocation_id = r.hget(trip_id_pulocation_hs_key, msg_dict["trip_id"])
                        if type(pulocation_id) == bytes:
                            pulocation_id = pulocation_id.decode('utf-8')
                        pulocation_active_trips_count_key = f"trip_id:{trip_id}:PULocationID:{pulocation_id}:active_count"
                        # Watch the key we are about to change.
                        pipe.watch(pulocation_active_trips_count_key, total_active_trips_key, trip_id_pulocation_hs_key)
                        # starting redis transcation
                        pipe.multi()
                        print(f">>>>>>>>>>>>starting redis transaction for dropoff data update")
                        # decr total active trips count
                        pipe.decr(total_active_trips_key, 1)
                        # decr active trips pulocation
                        pipe.decr(pulocation_active_trips_count_key, 1)
                        # delete trip_id from hset
                        pipe.hdel(trip_id_pulocation_hs_key, msg_dict["trip_id"])
                        pipe.execute()
                        print(f">>>>>>>>>>>>successfully finished redis transaction for dropoff data update")
                        break
                    except redis.WatchError:
                        # The transaction failed, so continue with the next attempt.
                        print(f">>>>>>>>>>>>redis transaction for dropoff data update failed, retrying...")
                        continue
        print(f"before commit event with offset = {msg.offset}")
        # manual commit offset 
        # consumer.commit()
        # consumer.commit({
        #     TopicPartition(msg.topic, msg.partition): OffsetAndMetadata(msg.offset+1, '')
        # })
        print(f"after commit event with offset = {msg.offset}")
        sleep(1)



if __name__ == '__main__':
    hw4_cli()