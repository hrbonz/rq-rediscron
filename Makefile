PKG_NAME:="rediscron"
PKG_VERSION:=$(shell uv tool run hatch version)

.PHONY: bump
bump: test
	git checkout main
	uv tool run hatch version patch
	git add rediscron/__meta__.py
	uv lock
	git add uv.lock
	pkg_ver=$$(uv tool run hatch version); \
	git commit -m "chore: version bump to $${pkg_ver}"

.PHONY: buildclean
buildclean:
	rm -rf *egg-info build dist

.PHONY: build
build: buildclean tox
	uv build

.PHONY: unittest
unittest:
	uv run python -m unittest discover

.PHONY: tox
tox:
	uv run tox

.PHONY: test
test: tox

.PHONY: fulltest
fulltest: tox unittest

.PHONY: docs
docs:
	make -C docs/ -f Makefile html SPHINXBUILD='uv run sphinx-build'
