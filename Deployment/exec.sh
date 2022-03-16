docker volume create dechallenge

docker build -t dechallenge:latest -f Deployment/Dockerfile .

docker run
    --name dechallenge
    --mount source=dechallenge,target=/usr/app
    dechallenge:latest
