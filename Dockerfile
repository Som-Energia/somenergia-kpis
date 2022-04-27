FROM python:3.8-slim-buster

RUN git clone git@github.com:Som-Energia/somenergia-kpis.git
RUN pip3 install -r somenergia-kpis/requirements.txt
