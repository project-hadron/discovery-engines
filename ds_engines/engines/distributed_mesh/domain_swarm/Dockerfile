FROM python:3.7

WORKDIR /domain/swarm
COPY requirements.txt /domain/swarm
RUN pip install --upgrade -r requirements.txt

COPY src /domain/swarm

CMD ["python", "swarm.py"]