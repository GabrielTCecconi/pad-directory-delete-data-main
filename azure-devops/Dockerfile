FROM public.ecr.aws/sam/build-nodejs18.x:1.144.0-20250911030134

USER root
USER root
RUN set -e; \
  if command -v dnf >/dev/null 2>&1; then \
    dnf -y update && \
    dnf -y install shadow-utils python3 python3-pip git tar gzip unzip sudo && \
    dnf clean all; \
  else \
    yum -y update && \
    yum -y install shadow-utils python3 python3-pip git tar gzip unzip sudo && \
    yum clean all; \
  fi

RUN useradd -m -u 1000 azp || true
RUN pip3 install --no-cache-dir --upgrade aws-sam-cli
ENV PATH="/usr/local/bin:${PATH}"