# Release process

* Ensure build is green
* Update version in `pyproject.toml` and `pika/__init__.py`
* Commit changes to `main` branch and push:
    ```
    git commit -a -m 'pika 2.0.0' && git push
    ```
* Ensure build is green
* Tag version (using your GPG keyid) and push:
    ```
    git tag -a -s -u B1B82CC0CF84BA70147EBD05D99DE30E43EAE440 -m 'pika 2.0.0' '2.0.0' && git push --tags
    ```
* Ensure build is green (if one triggered)
* Update `CHANGELOG.md`. Be sure to use the `--since-tag 1.3.0` argument:
    ```
    github_changelog_generator --token github_pat_MY_TOKEN --user pika --project pika --since-tag 1.3.0
    ```
    Review the generated file for invalid entries
* Commit changes to `main` branch and push:
    ```
    git commit -a -m 'pika 2.0.0 CHANGELOG' && git push
    ```
* Note the release's milestone number, then create release via GitHub Release UI or `gh` command like:
    ```
    # This creates artifacts to be uploaded:
    python -m build --sdist --wheel --outdir dist/ .

    # This creates the release on GitHub:
    gh release create '2.0.0' --notes 'https://pypi.org/project/pika/2.0.0/ | [GitHub milestone](https://github.com/pika/pika/milestone/20?closed=1)' ./dist/*
    ```
* Ensure the publish build succeeded. Example success output looks like this:
    ```
    Checking dist/pika-2.0.0-py2.py3-none-any.whl: PASSED
    Checking dist/pika-2.0.0.tar.gz: PASSED
    Uploading distributions to https://upload.pypi.org/legacy/
    Uploading pika-2.0.0-py2.py3-none-any.whl
    ...
    ...
    ...
    View at:
    https://pypi.org/project/pika/2.0.0/
    ```

* View the release on PyPI: https://pypi.org/project/pika/2.0.0/
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
    pip install pika==2.0.0
    python ./asynchronous_publisher_example.py
    ```

# Notes:

* `github_changelog_generator`

If you see this error when updating the changelog:

```
Warning: PR 46 merge commit was not found in the release branch or tagged git history and no rebased SHA comment was found
/path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator_fetcher.rb:86:in `block in associate_tagged_prs': No merge sha found for PR 46 via the GitHub API (StandardError)
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator_fetcher.rb:68:in `reject'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator_fetcher.rb:68:in `associate_tagged_prs'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator_fetcher.rb:48:in `add_first_occurring_tag_to_prs'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator.rb:152:in `fetch_issues_and_pr'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/github_changelog_generator-1.16.4/lib/github_changelog_generator/generator/generator.rb:55:in `block in compound_changelog'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/async-2.2.1/lib/async/task.rb:107:in `block in run'
        from /path/to/ruby/3.1.2/lib/ruby/gems/3.1.0/gems/async-2.2.1/lib/async/task.rb:243:in `block in schedule'
```

...you will have to edit the `lib/github_changelog_generator/generator/generator_fetcher.rb` file to change the `raise` to a `STDERR.puts`.
