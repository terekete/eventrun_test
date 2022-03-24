# Create the pubsub subscription  
```bash
gcloud pubsub subscriptions create cloud-run --topic cloud-run-topic \
   --push-endpoint=https://hellopubsub-tqjcrev3ga-nn.a.run.app/ \
   --push-auth-service-account=scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com --ack-deadline=600
```

# Build the image
```bash
gcloud builds submit --tag gcr.io/cio-exegol-lab-3dabae/hellopubsub
```

## Deploy the image to Cloud Run 
```bash
gcloud run deploy hellopubsub --image gcr.io/cio-exegol-lab-3dabae/hellopubsub --region northamerica-northeast1 --service-account scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com
```

## Test by submitting a pubsub message

```
'{"project_id":"cio-exegol-lab-3dabae", "dataset_id": "ge_test", "bucket_id": "cio-exegol-lab-3dabae-ge-test", "bigquery_dataset": "ge_test", "query": "SELECT * FROM ge_test.test_table1", "properties" :{"pickup_location_id": {"type": "integer"}, "vendor_id": {"enum": [1, 2, 4]}, "store_and_fwd_flag": {"type": "boolean"}, "passenger_count": {"type": "integer", "minimum": 0, "maximum": 130}}}'
```
