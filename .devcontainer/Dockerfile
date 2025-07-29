FROM spark:4.0.0-scala2.13-java21-python3-ubuntu

# Switch to root user for installations
USER root

COPY kbr5.conf /etc/krb5.conf
COPY config/fancygit/app_config /tmp

# Set proxy environment variables
ENV http_proxy=http://proxy.police.tas.gov.au:8080
ENV https_proxy=http://proxy.police.tas.gov.au:8080
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONBUFFERED=1

# Install Node.js repository
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -

# Fix permission issues and install system dependencies
RUN mkdir -p /var/lib/apt/lists/partial && \
    chmod 755 /var/lib/apt/lists/partial && \
    apt-get clean && \
    apt-get update && apt-get install -y \
    curl \
    git \
    wget \
    build-essential \
    fontconfig \
    make \
    htop \
    krb5-user \
    libkrb5-dev \
    unixodbc \
    unixodbc-dev \
    gnupg2 \
    ca-certificates \
    openssl \
    corkscrew \
    odbcinst \
    openssh-client \
    libmemcached-dev \
    nodejs \
    jq \
    sqlite3 \
    redis-tools \
    sudo \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && curl https://packages.microsoft.com/debian/12/prod/pool/main/m/msodbcsql18/msodbcsql18_18.3.2.1-1_amd64.deb -o msodbcsql18.deb \
    && ACCEPT_EULA=Y dpkg --install msodbcsql18.deb \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bash_profile \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && apt-get -y clean

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Copy requirements file and install dependencies with uv
COPY requirements.txt /tmp/requirements.txt
COPY add_requirements_to_uv.sh /tmp/add_requirements_to_uv.sh
RUN chmod +x /tmp/add_requirements_to_uv.sh
#RUN uv pip install --system -r /tmp/requirements.txt
RUN /tmp/add_requirements_to_uv.sh /tmp/requirements.txt
RUN npm install -g @anthropic-ai/claude-code
CMD ["/bin/bash"]
