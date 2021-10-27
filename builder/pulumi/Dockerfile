FROM python

COPY . .

# Download and install required tools.
RUN curl -L https://get.pulumi.com/ | bash
RUN pip3 install --upgrade pulumi
RUN pip3 install --upgrade pulumi-gcp
RUN pip3 install --upgrade Cerberus
RUN pip3 install --upgrade google-cloud-build
RUN pip3 install --upgrade google-cloud-storage

ENV PULUMI_SKIP_UPDATE_CHECK=false
ENV PATH=$PATH:/root/.pulumi/bin

RUN pulumi plugin install resource gcp v5.25.0

RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

ENTRYPOINT [ "pulumi", "version" ]
