.PHONY: test

static:
	pre-commit run --all-files

test:
	py.test test
