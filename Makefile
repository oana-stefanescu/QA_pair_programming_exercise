.PHONY: venv serve docker_rm docker_rmi docker_clean build docker_serve release

VENV_PIP=./venv/bin/pip
VENV_UVICORN=./venv/bin/uvicorn
VENV_PYTHON=./venv/bin/python
BI_PYPI=https://repo2.rz.adition.net/repository/bi-pipy/simple/
DOCKER_IMAGE:=partitioning-service
DOCKER_CONTAINER:=partitioning-service
HOST:=localhost
PORT:=8080
PROMETHEUS_MULTIPROC_DIR:=./prometheus-tmp

venv:
	python3 -m venv ./venv
	$(VENV_PIP) install --index-url $(BI_PYPI) --upgrade pip
	$(VENV_PIP) install --index-url setuptools -U
	$(VENV_PIP) install --index-url $(BI_PYPI) -r requirements.txt

serve:
	./prepare_prometheus_multiprocess_local.sh
	env prometheus_multiproc_dir="$(PROMETHEUS_MULTIPROC_DIR)" $(VENV_UVICORN) app.main:app --reload --port $(PORT) --host $(HOST) --log-level debug

run_tests:
	$(VENV_PYTHON) -m pytest app/tests --cov app

docker_rm:
	docker rm -f -v $(DOCKER_CONTAINER) || true

docker_rmi: docker_rm
	docker rmi -f $(DOCKER_IMAGE) || true

docker_clean: docker_rm docker_rmi

build: docker_clean
	docker build -t $(DOCKER_IMAGE) .

docker_serve: build
	docker run --name $(DOCKER_CONTAINER) -p $(PORT):80 -e DEFAULT_TIMEZONE=UTC $(DOCKER_IMAGE)

run_tests_docker: build
	docker run -e DEFAULT_TIMEZONE=UTC $(DOCKER_IMAGE) python -m pytest /app/app/tests --cov app

RELEASE_VERSION:=$(shell head -n 1 ./VERSION)
DOCKER_REPO_NAME:="repo2.rz.adition.net:5002"
DOCKER_VERSIONED_IMAGE:="repo2.rz.adition.net:5002/bi/partitioning-service:v$(RELEASE_VERSION)"
DOCKER_LATEST_IMAGE:="repo2.rz.adition.net:5002/bi/partitioning-service:latest"
RELEASE_TAG_NAME:=v$(RELEASE_VERSION)

release:
	@printf "Creating release for version %s\n" $(RELEASE_VERSION)
	@while [ -z "$$CONTINUE" ]; do \
        read -r -p "Type y if you are sure you want to push now. Make sure the version is updated (VERSION file) [y/N]: " CONTINUE; \
    done ; \
    [ $$CONTINUE = "y" ] || [ $$CONTINUE = "Y" ] || (echo "Exiting."; exit 1;)
	@echo "Building docker image"
	docker build -t $(DOCKER_VERSIONED_IMAGE) -t $(DOCKER_LATEST_IMAGE) .
	@echo "You need to be logged-in to $(DOCKER_REPO_NAME) in order to push images\n"
	@printf "Going to push %s and %s\n" $(REVERSE_MAP_BUILDER_DOCKER_FILE) $(DOCKER_LATEST_IMAGE)
	docker login $(DOCKER_REPO_NAME)
	docker push $(DOCKER_VERSIONED_IMAGE)
	docker push $(DOCKER_LATEST_IMAGE)
	@echo "Push succeeded"
	@printf "Creating git tag \"%s\"\n" $(RELEASE_TAG_NAME)
	git tag -a "$(RELEASE_TAG_NAME)" -m "release version $(RELEASE_TAG_NAME)"
	@echo "Pushing tag"
	git push origin "$(RELEASE_TAG_NAME)"
	@echo "Pushed tag"
