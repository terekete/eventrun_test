# Create the eventarc trigger
```bash
gcloud eventarc triggers create bq-cloud-run-trigger --location northamerica-northeast1 --service-account scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com --destination-run-service bqrun --event-filters type=google.cloud.audit.log.v1.written --event-filters methodName=google.cloud.bigquery.v2.JobService.InsertJob --event-filters serviceName=bigquery.googleapis.com 
```

# Build the image
```bash
gcloud builds submit --tag gcr.io/cio-exegol-lab-3dabae/bqrun
```

## Deploy the image to Cloud Run 
```bash
gcloud run deploy bqrun --image gcr.io/cio-exegol-lab-3dabae/bqrun --region northamerica-northeast1 --service-account scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com
```

## Trigger the eventarc


```python
INSERT test_dataset.new_data (run_id, run_ts) VALUES("ios", CURRENT_DATE())
```