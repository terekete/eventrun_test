FROM python

COPY . .

# Download and install required tools.
RUN curl -L https://get.pulumi.com/ | bash
RUN pip3 install pulumi==3.16.0
RUN pip3 install pulumi-gcp==5.25.0
RUN pip3 install --upgrade Cerberus
RUN pip3 install --upgrade google-cloud-build
RUN pip3 install --upgrade google-cloud-storage

ENV PULUMI_SKIP_UPDATE_CHECK=false
ENV PATH=$PATH:/root/.pulumi/bin

RUN pulumi plugin install resource gcp v5.25.0

# RUN curl -sSL https://sdk.cloud.google.com | bash
# ENV PATH $PATH:/root/google-cloud-sdk/bin

ENTRYPOINT [ "pulumi", "version" ]
