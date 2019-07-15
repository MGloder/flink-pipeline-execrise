#!/bin/sh

function create{
    kubectl create -f ./kubernetes/jobmanager-service.yaml
    kubectl create -f ./kubernetes/jobmanager-deployment.yaml
    kubectl create -f ./kubernetes/taskmanager-deployment.yaml
}

function delete {
    kubectl delete -f ./kubernetes/jobmanager-deployment.yaml
    kubectl delete -f ./kubernetes/taskmanager-deployment.yaml
    kubectl delete -f ./kubernetes/jobmanager-service.yaml
}

function proxy {
    echo "http://localhost:8001/api/v1/namespaces/default/services/flink-jobmanager:ui/proxy"
    kubectl proxy
}


