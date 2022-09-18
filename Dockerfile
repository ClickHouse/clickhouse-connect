FROM python:3.8-slim-buster as slim

ADD . /

RUN sed -i "s/developer_only/1.0.0/" setup.py
RUN python setup.py install

