# templage image from VMware
FROM bitnami/spark:latest

WORKDIR /testtest

USER root

COPY . .

VOLUME [ "/data" ]

RUN pip install --no-cache-dir -r requirements.txt

CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0

#docker build -t sparktest
#8080 port is jupyterlab
#docker run  -p 8888:8888 -t sparktest