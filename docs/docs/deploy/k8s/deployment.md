# Kubernetes Deployment Guide

This document describes how to deploy ArkFlow in a Kubernetes cluster.

## Prerequisites

- Kubernetes cluster (version >= 1.16)
- kubectl command-line tool
- Built ArkFlow Docker image

## Deployment Configuration

### ConfigMap

First, create a ConfigMap to store the ArkFlow configuration file:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: arkflow-config
data:
  config.yaml: |
    # Place your ArkFlow configuration here
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: arkflow
  labels:
    app: arkflow
spec:
  replicas: 1
  selector:
    matchLabels:
      app: arkflow
  template:
    metadata:
      labels:
        app: arkflow
    spec:
      containers:
      - name: arkflow
        image: arkflow:latest  # Replace with your image address
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: RUST_LOG
          value: "info"
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "512Mi"
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /app/etc
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: arkflow-config
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: arkflow
spec:
  selector:
    app: arkflow
  ports:
  - port: 8000
    targetPort: 8000
  type: ClusterIP  # Can be changed to NodePort or LoadBalancer as needed
```

## Deployment Steps

1. Create Configuration Files

```bash
# Create namespace (optional)
kubectl create namespace arkflow

# Apply ConfigMap
kubectl apply -f configmap.yaml
```

2. Deploy Application

```bash
# Deploy Deployment
kubectl apply -f deployment.yaml

# Deploy Service
kubectl apply -f service.yaml
```

3. Verify Deployment

```bash
# Check Pod status
kubectl get pods -l app=arkflow

# Check service status
kubectl get svc arkflow
```

## Configuration Details

- **Image Configuration**: In the Deployment configuration, replace `image: arkflow:latest` with your actual image address
- **Environment Variables**: Environment variables can be configured through the env field, currently configured with RUST_LOG=info
- **Port Configuration**: Service exposes port 8000 by default
- **Configuration File**: Mounted to the container's /app/etc directory via ConfigMap
- **Resource Limits**: Default resource requests and limits are set to prevent resource contention
- **Health Checks**: Liveness and readiness probes are configured to ensure proper application health monitoring

## Important Notes

1. Ensure the configuration file format in ConfigMap is correct
2. Adjust the number of replicas according to actual needs
3. Choose appropriate Service type based on your environment
4. Adjust resource limits according to your application's actual resource consumption
5. Modify health check endpoints to match your application's actual health check endpoints

## Persistent Storage (Optional)

If your ArkFlow deployment requires persistent storage, you can add a PersistentVolumeClaim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: arkflow-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

Then update your Deployment to use this PVC:

```yaml
# Add to the volumes section
volumes:
- name: data
  persistentVolumeClaim:
    claimName: arkflow-data

# Add to the volumeMounts section of your container
volumeMount:
- name: data
  mountPath: /app/data
```

## Troubleshooting

If the service fails to run properly after deployment, you can check the issues using these commands:

```bash
# View Pod logs
kubectl logs -l app=arkflow

# View detailed Pod information
kubectl describe pod -l app=arkflow
```