import os
import re
import csv
import sys
import json
import uuid
import gzip
import click
import aiohttp
import asyncio
from datetime import datetime, timedelta
from pymongo import MongoClient
from io import BytesIO
from pymongo.errors import BulkWriteError


@click.group()
def hw3_mongo_cli():
    pass

@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw4", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="taxi", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.option('--input-csv', type=str, default=None)
@click.argument('mongodb_connection_string', type=str)
def task_1(db_name, col_name, input_csv, mongodb_connection_string):
    if input_csv != None:
        if not os.path.exists(input_csv):
            print(f"task_1: {input_csv} No such file or directory", file=sys.stderr)
            sys.exit(1)
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name does not exist
    if col_name in db.list_collection_names():
        print(f"Error: collection with name '{col_name}' already exists in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    # get mongo collection
    collection = db[col_name]
    with open(input_csv) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            record = row
            record["_id"] = str(uuid.uuid4())
            # print(row['first_name'], row['last_name'])

    
    asyncio.run(async_get_gh_dataset(start_date, end_date, artifacts_dir, db, col_name, db_name))


async def async_get_gh_dataset(start_date, end_date, artifacts_dir, db, col_name, db_name, is_task_2 = False):
    """
    Загружает данные из GitHub Archive за указанный период.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for n in range(int((end_date - start_date).days)):
            date = start_date + timedelta(n)
            for hour in range(24):
                arch_file = f"{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
                # url = f"https://data.gharchive.org/{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
                url = f"https://data.gharchive.org/{arch_file}"
                tasks.append(asyncio.create_task(process_downloaded_file(session, url, artifacts_dir, arch_file, db, col_name, db_name, is_task_2)))
        await asyncio.gather(*tasks)
        print(f"successfully created collection '{col_name}' in db '{db_name}' with github dataset (https://data.gharchive.org/) from given range = {start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}")

async def process_downloaded_file(session, url, artifacts_dir, arch_file, db, col_name, db_name, is_task_2):
    async with session.get(url) as response:
        response.raise_for_status()
        if artifacts_dir != None:
            arch_file = f"{artifacts_dir}/{arch_file}"
        json_data_file = arch_file.rstrip('.gz')
        with open(json_data_file, 'wb') as downloaded_file:    
            with gzip.open(BytesIO(await response.read()), 'rb') as gz:
                downloaded_file.write(gz.read())
        # get mongo collection
        collection = db[col_name]
        with open(json_data_file, "r") as f:
            if is_task_2:
                try:
                    collection.insert_many(list(map(task_2_doc_transform, f)), ordered = False, bypass_document_validation = True)
                except BulkWriteError as bwe:
                    # print(f"Catched duplicate key error: {bwe.details}")
                    print(f"Catched duplicate key error, continue..." )
            else:
                collection.insert_many(list(map(json.loads, f)))
        print(f"data saved into {json_data_file} and inserted in collection '{col_name}' of db '{db_name}' mongodb")


def task_2_doc_transform(elem):
    doc = json.loads(elem)
    doc["_id"] = doc['id']
    return doc


@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.option('--artifacts-dir', type=str, default=None)
@click.argument('mongodb_connection_string', type=str)
@click.argument('date_range', type=str)
def task_2(db_name, col_name, artifacts_dir, mongodb_connection_string, date_range):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    if artifacts_dir != None:
        if not os.path.exists(artifacts_dir):
             print(f"task_2: {artifacts_dir} No such file or directory", file=sys.stderr)
             sys.exit(1)
    start_date_str, end_date_str = date_range.split(':')
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    
    asyncio.run(async_get_gh_dataset(start_date, end_date, artifacts_dir, db, col_name, db_name, True))


@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_3(db_name, col_name, mongodb_connection_string):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name exists
    if col_name not in db.list_collection_names():
        print(f"Error: collection with name '{col_name}' does not exist in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    # get mongo collection
    collection = db[col_name]
    pipeline = [
        {
            "$facet": {
                "n_no_id": [
                    { "$match": { "actor.id": { "$exists": False } } },
                    { "$count": "count" }
                ],
                "n_no_login": [
                    { "$match": { "actor.login": { "$exists": False } } },
                    { "$count": "count" }
                ],
                "n_one_login_many_id": [
                    { "$group": { "_id": "$actor.login", "uniqueIds": { "$addToSet": "$actor.id" } } },
                    { "$match": { "$expr": { "$gt": [{ "$size": "$uniqueIds" }, 1] } } },
                    { "$count": "count" }
                ],
                "n_one_id_many_login": [
                    { "$group": { "_id": "$actor.id", "uniqueLogins": { "$addToSet": "$actor.login" } } },
                    { "$match": { "$expr": { "$gt": [{ "$size": "$uniqueLogins" }, 1] } } },
                    { "$count": "count" }
                ]
            }
        },
        {
            "$project": {
                "n_no_id": { "$ifNull": [ { "$arrayElemAt": ["$n_no_id.count", 0] }, 0] },
                "n_no_login": { "$ifNull": [ { "$arrayElemAt": ["$n_no_login.count", 0] }, 0] },
                "n_one_login_many_id": { "$ifNull": [ { "$arrayElemAt": ["$n_one_login_many_id.count", 0] }, 0] },
                "n_one_id_many_login": { "$ifNull": [ { "$arrayElemAt": ["$n_one_id_many_login.count", 0] }, 0] }
            }
        }
    ]
    print(json.dumps(list(collection.aggregate(pipeline))[0], indent=4))


@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--in-col-name', type=str, default="gh_data", help="name of collection with input data in --db-name in mongodb (default 'gh_data')")
@click.option('--out-col-name', type=str, default="out_gh_data", help="name of collection with output data in --db-name in mongodb (default 'out_gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_3_dupl(db_name, in_col_name, out_col_name, mongodb_connection_string):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name exists
    if in_col_name not in db.list_collection_names():
        print(f"Error: collection with name '{in_col_name}' does not exist in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    # get mongo input collection
    inp_collection = db[in_col_name]
    # create first "temp_events" collection
    temp_events_pipeline = [
        {
            "$group":
            {
                "_id": "$actor.id",
                "user_id":
                {
                    "$first": "$actor.id"
                },
                "events":
                {
                    "$push": "$type"
                }
            }
        },
        {
            "$unwind": "$events"
        },
        {
            "$group":
            {
                "_id":
                {
                    "user_id": "$user_id",
                    "events": "$events"
                },
                "count":
                {
                    "$count": {}
                }
            }
        },
        {
            "$group":
            {
                "_id": "$_id.user_id",
                "events":
                {
                    "$push":
                    {
                        "type": "$_id.events",
                        "count": "$count"
                    }
                }
            }
        },
        {
            "$project":
            {
                "_id": 0,
                "user_id": "$_id",
                "events": 1
            }
        },
        {
            "$out": "temp_events"
        }
    ]
    inp_collection.aggregate(temp_events_pipeline)
    print(f"successfully created 'temp_events' collection in db '{db_name}'")
    temp_pushed_pipeline = [
        {
            "$group":
            {
                "_id": "$actor.id",
                "pushed_to":
                {
                    "$addToSet":
                    {
                        "$cond": 
                        [
                        {
                            "$eq": ["$type", "PushEvent"]
                        }, 
                        "$repo.name", 
                        None
                        ]
                    }
                }
            }
        },
        {
            "$project":
            {
                "_id": 0,
                "user_id": "$_id",
                "pushed_to": 1
            }
        },
        {
            "$out": "temp_pushed"
        }
    ]
    inp_collection.aggregate(temp_pushed_pipeline)
    print(f"successfully created 'temp_pushed' collection in db '{db_name}'")
    temp_events_by_ym_pipeline = [
        {
            "$project":
            {
                "_id": "$actor.id",
                "type": "$type",
                "year":
                {
                    "$year":
                    {
                        "$dateFromString":
                        {
                            "dateString": "$created_at"
                        }
                    }
                },
                "month":
                {
                    "$month":
                    {
                        "$dateFromString":
                        {
                            "dateString": "$created_at"
                        }
                    }
                }
            }
        },
        {
            "$group":
            {
                "_id":
                {
                    "user_id": "$_id",
                    "year": "$year",
                    "month": "$month"
                },
                "events":
                {
                    "$push":
                    {
                        "type": "$type"
                    }
                }
            }
        },
        {
            "$unwind": "$events"
        },
        {
            "$group":
            {
                "_id":
                {
                    "user_id": "$_id.user_id",
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "event": "$events.type"
                },
                "count":
                {
                    "$count": {}
                }
            }
        },
        {
            "$group":
            {
                "_id":
                {
                    "user_id": "$_id.user_id",
                    "year": "$_id.year",
                    "month": "$_id.month"
                },
                "events":
                {
                    "$push":
                    {
                        "type": "$_id.event",
                        "count": "$count"
                    }
                }
            }
        },
        {
            "$project":
            {
                "_id": 0,
                "user_id": "$_id.user_id",
                "events_by_ym":
                {
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "events": "$events"
                }
            }
        },
        {
            "$sort":
            {
                "events_by_ym.year": 1,
                "events_by_ym.month": 1
            }
        },
        {
            "$out": "temp_events_by_ym"
        }
    ]
    inp_collection.aggregate(temp_events_by_ym_pipeline)
    print(f"successfully created 'temp_events_by_ym' collection in db '{db_name}'")
    result_pipeline = [
        {
            "$lookup":
            {
                "from": "temp_pushed",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "pushed_data"
            }
        },
        {
            "$lookup":
            {
                "from": "temp_events_by_ym",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "res_data"
            }
        },
        {
            "$unwind": "$res_data"
        },
        {
            "$project":
            {
                "_id": 0,
                "user_id": 1,
                "events": "$events",
                "pushed_to":
                {
                    "$arrayElemAt": ["$pushed_data.pushed_to", 0]
                },
                "events_by_ym": "$res_data.events_by_ym"
            }
        },
        {
            "$out": out_col_name
        }
    ]
    # get mongo temp_events collection
    temp_events_coll = db["temp_events"]
    temp_events_coll.aggregate(result_pipeline)
    print(f"successfully created result collection '{out_col_name}' collection in db '{db_name}'")


@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_4():
    click.echo('Run subcommand `task_4()`')

@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_5(db_name, col_name, mongodb_connection_string):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name exists
    if col_name not in db.list_collection_names():
        print(f"Error: collection with name '{col_name}' does not exist in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    # get mongo collection
    collection = db[col_name]
    pipeline = [
        {
            "$facet": {
                "n_no_id": [
                    { "$match": { "actor.id": { "$exists": False } } },
                    { "$count": "count" }
                ],
                "n_no_login": [
                    { "$match": { "actor.login": { "$exists": False } } },
                    { "$count": "count" }
                ],
                "n_one_login_many_id": [
                    { "$group": { "_id": "$actor.login", "uniqueIds": { "$addToSet": "$actor.id" } } },
                    { "$match": { "$expr": { "$gt": [{ "$size": "$uniqueIds" }, 1] } } },
                    { "$count": "count" }
                ],
                "n_one_id_many_login": [
                    { "$group": { "_id": "$actor.id", "uniqueLogins": { "$addToSet": "$actor.login" } } },
                    { "$match": { "$expr": { "$gt": [{ "$size": "$uniqueLogins" }, 1] } } },
                    { "$count": "count" }
                ]
            }
        },
        {
            "$project": {
                "n_no_id": { "$ifNull": [ { "$arrayElemAt": ["$n_no_id.count", 0] }, 0] },
                "n_no_login": { "$ifNull": [ { "$arrayElemAt": ["$n_no_login.count", 0] }, 0] },
                "n_one_login_many_id": { "$ifNull": [ { "$arrayElemAt": ["$n_one_login_many_id.count", 0] }, 0] },
                "n_one_id_many_login": { "$ifNull": [ { "$arrayElemAt": ["$n_one_id_many_login.count", 0] }, 0] }
            }
        }
    ]
    print(json.dumps(list(collection.aggregate(pipeline))[0], indent=4))

@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_6():
    click.echo('Run subcommand `task_6()`')

@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--col-name', type=str, default="gh_data", help="name of collection in --db-name in mongodb (default 'gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_7():
    click.echo('Run subcommand `task_7()`')

@hw3_mongo_cli.command()
@click.option('--db-name', type=str, default="hw3", help="name of database in mongodb (default 'hw3')")
@click.option('--in-col-name', type=str, default="gh_data", help="name of collection with input data in --db-name in mongodb (default 'gh_data')")
@click.option('--out-col-name', type=str, default="out_gh_data", help="name of collection with output data in --db-name in mongodb (default 'out_gh_data')")
@click.argument('mongodb_connection_string', type=str)
def task_8(db_name, in_col_name, out_col_name, mongodb_connection_string):
    client = MongoClient(mongodb_connection_string)
    # get mongo database
    db = client[db_name]
    # check that given col_name exists
    if in_col_name not in db.list_collection_names():
        print(f"Error: collection with name '{in_col_name}' does not exist in db '{db_name}'", file=sys.stderr)
        sys.exit(1)
    # get mongo collection
    collection = db[in_col_name]
    regex = re.compile("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$")
    # regex = Regex.from_native(pattern)
    pipeline = [
        {
            "$match":
            {
                "type": "PushEvent"
            }
        },
        {
            "$unwind": "$payload.commits"
        },
        {
            "$addFields":
            {
                "domain":
                {
                    "$regexMatch":
                    {
                        "input": "$payload.commits.author.email",
                        "regex": regex
                    }
                }
            }
        },
        {
            "$match":
            {
                "domain": True
            }
        },
        {
            "$group":
            {
                "_id":
                {
                    "email": "$payload.commits.author.email",
                    "repo_name": "$repo.name"
                },
                "count":
                {
                    "$sum": 1
                }
            }
        },
        {
            "$group":
            {
                "_id": "$_id.email",
                "count":
                {
                    "$sum": 1
                }
            }
        },
        {
            "$project":
            {
                "_id": 0,
                "host": "$_id",
                "count": 1
            }
        },
        {
            "$sort":
            {
                "count": -1
            }
        },
        {
            "$out": out_col_name
        }
    ]
    collection.aggregate(pipeline)
    print(f"successfully created result collection '{out_col_name}' collection in db '{db_name}'")
    # print(json.dumps(list(collection.aggregate(pipeline))[0], indent=4))

if __name__ == '__main__':
    hw3_mongo_cli()