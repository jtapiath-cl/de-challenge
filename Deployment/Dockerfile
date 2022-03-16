FROM python:latest
LABEL creator="Javier Tapia Thompson"
COPY ./Deployment/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
WORKDIR /usr/app
COPY ../data ./data
COPY ../src ./src
CMD [ "python", "./src/main.py" ]
