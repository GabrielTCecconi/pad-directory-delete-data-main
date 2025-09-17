# Base oficial do SAM p/ Node 18
FROM public.ecr.aws/sam/build-nodejs18.x:1.144.0-20250911030134

USER root
# AL2023 usa dnf. AL2 usa yum. Tente dnf, caia p/ yum.
RUN (command -v dnf >/dev/null 2>&1 && dnf -y update && \
     dnf -y install shadow-utils python3 python3-pip git tar gzip unzip && \
     dnf clean all) || \
    (yum -y update && \
     yum -y install shadow-utils python3 python3-pip git tar gzip unzip && \
     yum clean all)

# Instala SAM CLI de forma global
RUN pip3 install --no-cache-dir aws-sam-cli
ENV PATH="/usr/local/bin:${PATH}"