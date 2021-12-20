.PHONY: clean clean-test clean-pyc clean-build docs help
init:
	pip install -r requirements.txt

test:
	nosetests tests

lint/flake8: ## check style with flake8
	flake8 entity_resolution tests

lint: lint/flake8

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/entity_resolution.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ entity_resolution
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html