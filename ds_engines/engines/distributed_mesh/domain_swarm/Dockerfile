FROM python:3.7

WORKDIR /root/users/ddsm
COPY requirements.txt /root/users/ddsm
RUN pip install --upgrade -r requirements.txt

COPY src /root/users/ddsm

CMD ["python", "swarm.py"]