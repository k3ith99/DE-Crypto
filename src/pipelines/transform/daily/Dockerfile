FROM python:3.9-bullseye 


ARG SPARK_VERSION=3.5.4

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*do

# Copy the requirements.txt file from the host machine to the container
COPY ./requirements.txt .
COPY ./daily_transform.py .
COPY ./main.env .


# Install the dependencies from the requirements.txt file
RUN pip install -r requirements.txt 

#move into jar the pyspark jar stuff
RUN cd /usr/local/lib/python3.9/site-packages/pyspark/jars && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://jdbc.postgresql.org/download/postgresql-42.7.5.jar

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["python", "daily_transform.py"]
