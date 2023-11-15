FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install requests pandas

WORKDIR /app

COPY ./*.py ./

CMD ["python3", "install_import.py"]
