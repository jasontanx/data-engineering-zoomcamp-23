FROM prefecthq/prefect

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
RUN mkdir -p /opt/prefect/data/yellow
