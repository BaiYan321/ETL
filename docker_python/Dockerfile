# build image starting with Python 3.10 image. If we don't have 3.10, will use the latest image
FROM python:3.9-slim-buster

# Set the working directory for the container. It's IN the container
# You don't need to create this folder
# 当前工作路径，在docker里，而不是在local的文件夹
WORKDIR /test

# copy all the file to the directory
# 把local当前所有的文件，copy到 WORKDIR的里头，即放入docker里,即包含.env
COPY . /test

#copy specific file
#COPY requirements.txt .
#COPY api.py ./

# 作为docker container里的路径，即一个存储空间
# 如果这里不定影，就会使用默认的
# df -h可以显示在container里的路径 /dev/sdd
# Mount相当于放到disk上的哪个区域0

#VOLUME /out

# 尝试把volume变成本地的directory，这个方法不work
#VOLUME ["C:/Users/ryanb/d2l-en/ETL/ETL/docker_python/test"]

# install all the packages in the requirement file
# could be written as
#RUN pip install python-dotenv
#RUN pip install datetime
#RUN pip install requests

RUN pip install -r requirements.txt

#command call python to run the py.file
CMD ["python", "api.py"]

#这个指令，让docker一直跑，然后通过docker里的container输入python .py 的方式让它运行
#CMD ["tail", "-f", "/dev/null"]

#在跑完之后，使用docker cp的方式，复制container ID里某个地址的文件，到本地
# docker cp 519b73edfd09:test/nyt.json C:\Users\ryanb\d2l-en\ETL\ETL\docker_python
# 另外一种方式是从硬盘里copy到本地路径