Note: 
- Ignore any warning for now, for development purpose only
- Warning is important, please do not ignore warning in production


## Install MLflow using Docker

I created a new docker-compose.yaml based on this repo: https://github.com/sachua/mlflow-docker-compose.git

change directory to mlflow-docker-compose
Inside the directory, run: 
docker-compose up -d --build

## MLflow UI
Akses MLflow UI di http://localhost:5000/

## Minio
Akses Minio UI di http://localhost:9001/browser/mlflow


## Start tracking model deployment

check current running docker container name
docker ps
get the CONTAINER ID and copy paste in <mycontainer>
docker exec -it <mycontainer> bash

run above command in your local terminal to switch to docker cli


## Run script
in the docker cli, change dir to /opt/mlflow/scripts and you will find the mounted script from your local computer.
run python3 1_example_autolog.py



3_elasticwinenet_train.py <alpha> <l1_ratio>


mlflow run https://github.com/mlflow/mlflow-example.git -P alpha=0.42


## Serving models

mlflow models serve -m s3://mlflow/1/6e572bc4f46c4cf082aaa723b4bdae91/artifacts/model -p 1234

curl -X POST -H "Content-Type:application/json" --data '{"dataframe_split": {"columns":["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"],"data":[[6.2, 0.66, 0.48, 1.2, 0.029, 29, 75, 0.98, 3.33, 0.39, 12.8]]}}' http://127.0.0.1:1234/invocations



https://mlflow.org/docs/latest/tutorials-and-examples/tutorial.html

