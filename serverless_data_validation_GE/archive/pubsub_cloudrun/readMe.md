# Create the pubsub subscription  
```bash
gcloud pubsub subscriptions create cloud-run --topic cloud-run-topic \
   --push-endpoint=https://hellopubsub-tqjcrev3ga-nn.a.run.app/ \
   --push-auth-service-account=scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com
```

# Build the image
```bash
gcloud builds submit --tag gcr.io/cio-exegol-lab-3dabae/hellopubsub
```

## Deploy the image to Cloud Run 
```bash
gcloud run deploy hellopubsub --image gcr.io/cio-exegol-lab-3dabae/hellopubsub --region northamerica-northeast1 --service-account scheduler-test@cio-exegol-lab-3dabae.iam.gserviceaccount.com
```
