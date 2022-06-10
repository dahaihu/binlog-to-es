docker pull prom/prometheus

docker run -d --name prometheus -p 9090:9090 --network host -v \
/home/hsc/GolandProjects/go/src/binlog-to-es/docker/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml


docker run -d --name grafana --network host grafana/grafana