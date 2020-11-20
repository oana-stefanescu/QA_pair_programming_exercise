# Introduction

# Run API
You can run the API locally by using the `make serve` target or run it in docker 
by using the `make docker_serve` target.

# Build and Deploy
The project is deployed via a Docker image.
To create a new release first adjust the VERSION file (to a version that has not been used before).
Make sure that everything, including the VERSION file, is committed.

Then use `make release`. This will create a git tag, build the docker image and upload
it to Nexus.

You can find the complete deploy manual [here](https://teamwork.vimico.com/confluence/pages/viewpage.action?pageId=87182298).
# Tests
