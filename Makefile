.PHONY: clean clean-test clean-pyc clean-build docdef help
init:
	pip install -r requirements.txt

test:
	nosetests tests

lint/flake8: ## check style with flake8
	flake8 entity_resolution tests

lint: lint/flake8

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docdef/entity_resolution.rst
	rm -f docdef/modules.rst
	sphinx-apidoc -o docs/ entity_resolution
	$(MAKE) -C docdef clean
	$(MAKE) -C docdef html
	$(BROWSER) docs/html/index.html