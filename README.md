# Flink-pipeline-execrise




## Flink cluster with kubernetes
### Pre-requisite
Install [kubernetes]() and [minikube](https://kubernetes.io/docs/setup/learning-environment/minikube/) 
### Create flink cluster
In flink-cluster.sh run create

### Stop flink cluster
In flink-cluster.sh run delete

### Access flink Web UI
In flink-cluster.sh run proxy

### Check pod status
```bash
kubectl get pod
```
______________
reference: [flink-docs](https://ci.apache.org/projects/flink/flink-docs-release-1.8/ops/deployment/kubernetes.html#session-cluster-resource-definitions)


## Watermark

![watermark](./src/resources/watermark.png)
