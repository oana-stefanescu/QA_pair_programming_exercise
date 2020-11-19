.PHONY: venv serve docker_rm docker_rmi docker_clean build docker_serve

VENV_PIP=./venv/bin/pip
VENV_UVICORN=./venv/bin/uvicorn
BI_PYPI=https://repo2.rz.adition.net/repository/bi-pipy/simple/
DOCKER_IMAGE:=partitioning-service
DOCKER_CONTAINER:=partitioning-service
HOST:=localhost
PORT:=8080
PROMETHEUS_MULTIPROC_DIR:=./prometheus-tmp

venv:
	python3 -m venv ./venv
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install setuptools -U
	$(VENV_PIP) install --index-url $(BI_PYPI) -r requirements.txt

serve:
	./prepare_prometheus_multiprocess_local.sh
	env prometheus_multiproc_dir="$(PROMETHEUS_MULTIPROC_DIR)" $(VENV_UVICORN) app.main:app --reload --port $(PORT) --host $(HOST) --log-level debug

docker_rm:
	docker rm -f -v $(DOCKER_CONTAINER) || true

docker_rmi: docker_rm
	docker rmi -f $(DOCKER_IMAGE) || true

docker_clean: docker_rm docker_rmi

build: docker_clean
	docker build -t $(DOCKER_IMAGE) .

docker_serve: build
	docker run -it --name $(DOCKER_CONTAINER) -d -p $(PORT):80 -e DEFAULT_TIMEZONE=UTC $(DOCKER_IMAGE)
