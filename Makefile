.PHONY: venv serve docker_rm docker_rmi docker_clean build docker_serve

VENV_PIP=./venv/bin/pip
BI_PYPI=https://repo2.rz.adition.net/repository/bi-pipy/simple/
DOCKER_IMAGE:=partitioning-service
DOCKER_CONTAINER:=partitioning-service
PORT:=5000

venv:
	python3 -m venv ./venv
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install setuptools -U
	$(VENV_PIP) install --index-url $(BI_PYPI) -r requirements.txt

serve: venv
	. ./venv/bin/activate
	uvicorn app.main:app --reload --port $(PORT) --host 0.0.0.0 --log-level debug

docker_rm:
	docker rm -f -v $(DOCKER_CONTAINER) || true

docker_rmi: docker_rm
	docker rmi -f $(DOCKER_IMAGE) || true

docker_clean: docker_rm docker_rmi

build: docker_clean
	docker build -t $(DOCKER_IMAGE) .

docker_serve: build
	docker run -it --name $(DOCKER_CONTAINER) -d -p $(PORT):80 -e DEFAULT_TIMEZONE=UTC $(DOCKER_IMAGE)
