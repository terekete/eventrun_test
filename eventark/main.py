import os
from flask import Flask, request
import json
from google.cloud import bigquery
from google.oauth2 import service_account

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    # Gets the Payload data from the Audit Log
    content = request.json
    try:
        print(content)
        ds = content['resource']['labels']['dataset_id']
        proj = content['resource']['labels']['project_id']
        tbl = content['protoPayload']['resourceName']
        rows = int(content['protoPayload']['metadata']['tableDataChange']['insertedRowsCount'])
        if ds == 'test_dataset' and tbl.endswith('tables/new_data') and rows > 0:
            query = create_agg()
            return "table created", 200
    except:
        # if these fields are not in the JSON, ignore
        pass
    return "ok", 200


# [END eventarc_gcs_handler]

def create_agg():
    client=bigquery.Client()
    query = """
INSERT test_dataset.data (run_id) VALUES("call_success")
# INSERT `cto-datahub-bi-staging-pr-3437.source_data.data` (run_id, run_ts) VALUES("ios", CURRENT_DATE())
    """
    client.query(query)
    return query

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
# [END eventarc_gcs_server]    
    