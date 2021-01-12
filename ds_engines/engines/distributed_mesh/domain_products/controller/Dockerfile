FROM python:3.8

WORKDIR /root/hadron
COPY requirements.txt /root/hadron
RUN pip install --upgrade -r requirements.txt

COPY ./src /root/hadron

CMD ["python", "controller.py"]