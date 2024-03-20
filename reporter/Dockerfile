FROM python:latest
WORKDIR /reporter
COPY requirements.txt /reporter
RUN pip3 install --upgrade -r requirements.txt --no-cache-dir
RUN pip3 install zeep==4.2.1

# Set the timezone.
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY reporter.py /reporter
ENTRYPOINT ["python3"]
CMD ["reporter.py"]
