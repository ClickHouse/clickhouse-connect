FROM python:3.10

ADD . /

RUN pip install --upgrade pip; \
    pip install cython; \
    pip install -v .
# RUN pip install -v --index-url https://test.pypi.org/simple/ clickhouse-connect
CMD ["python", "playtest.py"]
