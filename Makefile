.PHONY: test

interop_image:
	@docker login -u "$(DOCKER_USER)" -p "$(DOCKER_PASS)"
	docker build -t nameko/nameko-grpc-interop -f test/interop.docker test
	docker push nameko/nameko-grpc-interop

static:
	pre-commit run --all-files

test:
	py.test test
