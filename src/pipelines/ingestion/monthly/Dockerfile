FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file from the host machine to the container
COPY ./requirements.txt /app/requirements.txt
COPY ./minio_monthly.py /app/

#move into jar the pyspark jar stuff

# Install the dependencies from the requirements.txt file
RUN pip install -r /app/requirements.txt
#move into jar the pyspark jar stuff

ENTRYPOINT ["tail", "-f", "/dev/null"]
#CMD ["python", "minio_monthly"]
