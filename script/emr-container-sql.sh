# ./emr-container-sql.sh s3://$S3BUCKET/testsql/ 2 12 Name=eks-202402220-od-arm-sql1 od-arm od-arm
export SQL_S3_PATH=$1
export CORES=$2
export MEMORY=$3
export TAG=$4
export DRIVER_POD_TEMPLATE="driver-$5-pod-template.yaml"
export EXECUTOR_POD_TEMPLATE="executor-$6-pod-template.yaml"
# export TIMESTAMP=`date +%y-%m-%d-%H-%M-%S`

export CORES="${CORES:-4}"
export MEMORY="${MEMORY:-8}"

# calculate 30% overhead and ceil
export MEMORY_OVERHEAD=$(((${MEMORY}*25+(100-1))/100))
export EXEC_MEMORY=$((${MEMORY}-${MEMORY_OVERHEAD}))

export EKSCLUSTER_NAME="${EKSCLUSTER_NAME:-emr-eks-workshop}"
export ACCOUNTID="${ACCOUNTID:-$(aws sts get-caller-identity --query Account --output text)}"
export AWS_REGION="${AWS_REGION:-$(curl -s 169.254.169.254/latest/dynamic/instance-identity/document | jq -r '.region')}"
export S3BUCKET="${S3BUCKET:-${EKSCLUSTER_NAME}-${ACCOUNTID}-${AWS_REGION}}"

export VIRTUAL_CLUSTER_ID="${VIRTUAL_CLUSTER_ID:-406xiic2jaj0130dwo9fr053v}"
export EMR_ROLE_ARN=arn:aws:iam::$ACCOUNTID:role/$EKSCLUSTER_NAME-execution-role
export ECR_URL="$ACCOUNTID.dkr.ecr.$AWS_REGION.amazonaws.com"
export MAIN_SCRIPT_PATH="s3://${S3BUCKET}/script/emr-container-sql.py"

aws emr-containers start-job-run \
  --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
  --name emr-on-eks-${CORES}vcpu-${MEMORY}gb  \
  --execution-role-arn $EMR_ROLE_ARN \
  --release-label emr-6.10.0-latest \
  --tags ''$TAG'' \
  --job-driver '{
  "sparkSubmitJobDriver": {
      "entryPoint": "'${MAIN_SCRIPT_PATH}'",
      "entryPointArguments":["'$AWS_REGION'", "'$SQL_S3_PATH'"],
      "sparkSubmitParameters": "--conf spark.executor.instances=20 --conf spark.driver.cores='$CORES' --conf spark.driver.memory='$EXEC_MEMORY'g --conf spark.executor.cores='$CORES' --conf spark.executor.memory='$EXEC_MEMORY'g"}}' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
          "spark.kubernetes.container.image":  "'$ECR_URL'/eks-spark-benchmark:emr6.10",
          "spark.kubernetes.driver.podTemplateFile": "s3://'$S3BUCKET'/pod-template/'$DRIVER_POD_TEMPLATE'",
          "spark.kubernetes.executor.podTemplateFile": "s3://'$S3BUCKET'/pod-template/'$EXECUTOR_POD_TEMPLATE'",
          "spark.dynamicAllocation.enabled": "true",
          "spark.network.timeout": "2000s",
          "spark.executor.heartbeatInterval": "300s",
          "spark.kubernetes.executor.limit.cores": "'$CORES'",
          "spark.executor.memoryOverhead": "'$MEMORY_OVERHEAD'G",
          "spark.driver.memoryOverhead": "'$MEMORY_OVERHEAD'G",
          "spark.kubernetes.executor.podNamePrefix": "karpenter-'$CORES'vcpu-'$MEMORY'gb",
          "spark.executor.defaultJavaOptions": "-verbose:gc -XX:+UseG1GC",
          "spark.driver.defaultJavaOptions": "-verbose:gc -XX:+UseG1GC",
          "spark.metrics.conf": "/etc/metrics/conf/metrics.properties",
          "spark.ui.prometheus.enabled":"true",
          "spark.executor.processTreeMetrics.enabled":"true",
          "spark.kubernetes.driver.annotation.prometheus.io/scrape":"true",
          "spark.kubernetes.driver.annotation.prometheus.io/path":"/metrics/executors/prometheus/",
          "spark.kubernetes.driver.annotation.prometheus.io/port":"4040",
          "spark.kubernetes.driver.service.annotation.prometheus.io/scrape":"true",
          "spark.kubernetes.driver.service.annotation.prometheus.io/path":"/metrics/driver/prometheus/",
          "spark.kubernetes.driver.service.annotation.prometheus.io/port":"4040",
          "spark.metrics.conf.*.sink.prometheusServlet.class":"org.apache.spark.metrics.sink.PrometheusServlet",
          "spark.metrics.conf.*.sink.prometheusServlet.path":"/metrics/driver/prometheus/",
          "spark.metrics.conf.master.sink.prometheusServlet.path":"/metrics/master/prometheus/",
          "spark.metrics.conf.applications.sink.prometheusServlet.path":"/metrics/applications/prometheus/"
         }
      }
    ],
    "monitoringConfiguration": {
      "persistentAppUI":"ENABLED",
      "s3MonitoringConfiguration": {
        "logUri": "'"s3://$S3BUCKET"'/logs/spark/"
      }
    }
  }'