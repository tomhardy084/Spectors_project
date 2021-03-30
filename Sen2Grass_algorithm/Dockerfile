FROM osgeo/gdal:ubuntu-full-latest

# Enter your name and e mailadres
LABEL maintainer = "<yourname@email.com>"

# Add pip package manager
RUN apt update && apt-get install -y \
    python3-pip

# Install Python 
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
ADD requirements.txt /code/
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
ADD . /code/

EXPOSE 3000
CMD ["python3", "/code/main.py"]