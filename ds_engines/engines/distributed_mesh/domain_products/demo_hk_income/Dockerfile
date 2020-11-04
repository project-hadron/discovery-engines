FROM python:3.7

WORKDIR /root/ddsm
COPY requirements.txt /root/ddsm
RUN pip install --upgrade -r requirements.txt

COPY ./src /root/ddsm

CMD ["python", "controller.py"]