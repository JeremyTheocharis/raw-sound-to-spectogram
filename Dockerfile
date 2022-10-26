FROM python:3.10.8

WORKDIR /app

COPY ./main.py /app/main.py
RUN pip3 install pandas numpy matplotlib scipy kafka-python

ENTRYPOINT ["python3"]
CMD ["-u", "main.py"]
