#stage 1
FROM python:3.11-bullseye
SHELL [ "/bin/bash", "-c" ]
WORKDIR /app
COPY . ./code
# Install required packages
RUN apt update && apt install -f zip
RUN ./code/build.sh