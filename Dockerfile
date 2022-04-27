FROM python:3.8-slim-buster

RUN apt-get update  
RUN apt-get install -y git
RUN apt-get install -y python3-pip
RUN git clone https://github.com/Som-Energia/somenergia-kpis.git
RUN pip3 install -r somenergia-kpis/requirements.txt
