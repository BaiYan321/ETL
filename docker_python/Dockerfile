# build image starting with Python 3.10 image. If we don't have 3.10, will use the latest image
FROM python:3.9-slim-buster

# Set the working directory for the container. It's IN the container
# You don't need to create this folder
WORKDIR /test
# copy all the file to the directory
COPY . /test

COPY requirements.txt .

COPY api.py ./
RUN mkdir -p /test/output

# install all the packages in the requirement file
# could be written as
#RUN pip install python-dotenv
#RUN pip install datetime
#RUN pip install requests

RUN pip install -r requirements.txt

#command call python to run the py.file
CMD ["python", "api.py"]