import os
from flask import Flask, request
import json
from google.cloud import bigquery

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    content = request.json
    try:
        print(content)
        ds = content['resource']['labels']['dataset_id']
        proj = content['resource']['labels']['project_id']
        tbl = content['protoPayload']['resourceName']
        rows = int(content['protoPayload']['metadata']['tableDataChange']['insertedRowsCount'])
        if ds == 'cloud_run_tmp' and tbl.endswith('tables/cloud_run_trigger') and rows > 0:
            query = create_agg()
            return "table created", 200
    except:
        pass
    return "ok", 200


def create_agg(query):
    client = bigquery.Client()
    query = query
    client.query(query)
    return query
