FROM python:latest
WORKDIR /service
COPY requirements.txt /service
RUN pip3 install -r requirements.txt --no-cache-dir
COPY sertvice.py /service
ENTRYPOINT ["python3"]
CMD ["service.py"]
