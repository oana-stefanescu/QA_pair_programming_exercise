# Introduction

# Run API
You can run the API locally by using the `make serve` target or run it in docker 
by using the `make docker_serve` target.

Note that before you have to create a virtual environment in `./venv`.
You can also created it with the `Makefile`:

```bash
make venv
```

# Build and Deploy
The project is deployed via a Docker image.
To create a new release first adjust the VERSION file (to a version that has not been used before).
Make sure that everything, including the VERSION file, is committed.

Then use `make release`. This will create a git tag, build the docker image and upload
it to Nexus.

You can find the complete deploy manual [here](https://teamwork.vimico.com/confluence/pages/viewpage.action?pageId=87182298).

# Tests
Tests are located in `app/tests`. They're standard `pytest` tests.

You can use the `Makefile` to run the tests locally (needs a virtual environment):

```bash
make run_tests
```

or in Docker:

```bash
make run_tests_docker
```
