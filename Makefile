format:
	poetry run black squids
	poetry run isort squids
	poetry run black tests
	poetry run isort tests


test:
	poetry run mypy squids tests
	poetry run python -m unittest discover


docs:
	poetry run sphinx-apidoc -f -o docs squids
	poetry run sphinx-build -b html docs docs/_build


.PHONY: format test docs
