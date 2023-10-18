w.PHONY: test install python-dependencies docker-dependencies help clean build pypi pypi-test

# SETUP / INSTALLATION -------------------------------------------------------------------------------------------------
python-dependencies:
	pip install -r requirements.txt

docker-dependencies:
	docker pull obolibrary/odkfull:dev

install: python-dependencies docker-dependencies

# TEST -----------------------------------------------------------------------------------------------------------------
test:
	python -m unittest discover -s test/

# PACKAGE MANAGEMENT ---------------------------------------------------------------------------------------------------
clean:
	rm -rf ./dist;
	rm -rf ./build;
	rm -rf ./*.egg-info

# todo: change from `setuptools` to `biuld` (see table): https://blog.ganssle.io/articles/2021/10/setup-py-deprecated.html
build: clean
	python setup.py sdist bdist_wheel

pypi: build
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*;

pypi-test: build
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*


# HELP -----------------------------------------------------------------------------------------------------------------
help:
	@echo "-----------------------------------"
	@echo "	Command reference: OMOP2OWL"
	@echo "-----------------------------------"
	@echo "install:"
	@echo "Installation."
