# Release process

* Ensure build is green
* Update version in `pyproject.toml` and `pika/__init__.py`
* Update `CHANGELOG.rst` and copy file to `docs/version_history.rst`
* Commit changes to `main` branch and push:
    ```
    git commit -a -m 'pika 1.3.0' && git push
    ```
* Ensure build is green
* Tag version (using your GPG keyid) and push:
    ```
    git tag -a -s -u B1B82CC0CF84BA70147EBD05D99DE30E43EAE440 -m 'pika 1.3.0' '1.3.0' && git push --tags
    ```
* Ensure build is green (if one triggered)
* Note the release's milestone number, then create release via GitHub Release UI or `gh` command like:
    ```
    # This creates artifacts to be uploaded:
    python -m build --sdist --wheel --outdir dist/ .

    # This creates the release on GitHub:
    gh release create '1.3.0' --notes 'https://pypi.org/project/pika/1.3.0/%0A[GitHub milestone](https://github.com/pika/pika/milestone/19?closed=1)' ./dist/*
    ```
* Ensure the publish build succeeded. Example success output looks like this:
    ```
    Checking dist/pika-1.3.0rc2-py2.py3-none-any.whl: PASSED
    Checking dist/pika-1.3.0rc2.tar.gz: PASSED
    Uploading distributions to https://upload.pypi.org/legacy/
    Uploading pika-1.3.0rc2-py2.py3-none-any.whl
    ...
    ...
    ...
    View at:
    https://pypi.org/project/pika/1.3.0rc2/
    ```

* View the release on PyPI: https://pypi.org/project/pika/1.3.0/
* Ensure the release works!
  * Start RabbitMQ
    ```
    docker run --pull --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:3-management-alpine
    ```
  * Run example Pika program
    ```
    cd path/to/pika/examples
    python -m venv venv
    source ./venv/bin/activate
    pip install pika==1.3.0
    python ./asynchronous_publisher_example.py
    ```
