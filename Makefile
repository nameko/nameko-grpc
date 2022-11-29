.PHONY: test

interop_image:
	@docker login -u "$(DOCKER_USER)" -p "$(DOCKER_PASS)"
	docker build -t nameko/nameko-grpc-interop -f test/interop.docker test
	docker push nameko/nameko-grpc-interop

static:
	pre-commit run --all-files

test:
	nameko test test -v --timeout 120 -o log_cli=true -o log_cli_level=10

coverage:
	coverage run -m nameko test test -v
	coverage report