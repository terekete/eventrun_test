FROM python

COPY . .

# Download and install required tools.
RUN curl -L https://get.pulumi.com/ | bash
RUN pip3 install --upgrade google-cloud-eventarc
RUN pip3 install --upgrade google-cloud-bigquery
RUN pip3 install --upgrade Cerberus

ENV PULUMI_SKIP_UPDATE_CHECK=false
ENV PATH=$PATH:/root/.pulumi/bin

ENTRYPOINT [ "python", "-m" ]
