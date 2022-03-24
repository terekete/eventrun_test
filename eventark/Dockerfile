FROM python:3.10-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

COPY . .

# Download and install required tools.
RUN pip3 install --upgrade google-cloud-bigquery
RUN pip3 install --upgrade google-auth

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

#CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
CMD exec gunicorn --bind :$PORT main:app

# CMD ["python", "main.py"]