# Release process

* Create `pika-1.4.3` branch
* Update `CHANGELOG.md`. Be sure to use the `--since-tag X.Y.Z` argument:
    ```
    github_changelog_generator --user pika --project pika --token $TOKEN --since-tag 1.4.2 --future-release 1.4.3 --release-branch 1.4.x
    ```
    Review the generated file for invalid entries. More than likely you will have to hand-edit `CHANGELOG.md`
* Update version in `pyproject.toml` and `pika/__init__.py`
* Commit changes to `pika-1.4.3` branch and push:
    ```
    git commit -a -m 'pika 1.4.3' && git push
    ```
* Open PR and ensure build is green
* Merge PR
* Update local `main`
* Tag version (using your GPG keyid) and push:
    ```
    git tag -a -s -u B1B82CC0CF84BA70147EBD05D99DE30E43EAE440 -m 'pika 1.4.3' '1.4.3' && git push --tags
    ```
* Ensure build is green (if one triggered)
* Note the release's milestone number, then create release via GitHub Release UI or `gh` command like:
    ```
    # This creates artifacts to be uploaded:
    python -m build --sdist --wheel --outdir dist/ .

    # This creates the release on GitHub:
    gh release create '1.4.3' --notes 'https://pypi.org/project/pika/1.4.3/ | [GitHub milestone](https://github.com/pika/pika/milestone/23?closed=1)' ./dist/*
    ```
* Ensure the publish build succeeded. Example success output looks like this:
    ```
    Checking dist/pika-1.4.3-py3-none-any.whl: PASSED
    Checking dist/pika-1.4.3.tar.gz: PASSED
    Uploading distributions to https://upload.pypi.org/legacy/
    Uploading pika-1.4.3-py3-none-any.whl
    ...
    ...
    ...
    View at:
    https://pypi.org/project/pika/1.4.3/
    ```

* View the release on PyPI: https://pypi.org/project/pika/1.4.3/
* Ensure the release works!
  * Start RabbitMQ
    ```
    docker run --pull --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:4-management-alpine
    ```
  * Run example Pika program
    ```
    cd path/to/pika/examples
    python -m venv venv
    source ./venv/bin/activate
    pip install pika==1.4.3
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
