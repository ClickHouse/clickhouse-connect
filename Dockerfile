FROM python:3.10

ADD playtest.py playtest.py

#RUN pip install --upgrade pip; \
#    pip install cython; \
#    pip install -v .
RUN pip install requests pytz
RUN pip install -v --index-url https://test.pypi.org/simple/ clickhouse-connect==0.2.8.post6
CMD ["python", "playtest.py"]
