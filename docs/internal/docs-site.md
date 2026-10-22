# The documentation site

Runbooks for the published site. `RELEASE.md` covers the release itself and says when a release touches the site; these are the operations that are not part of cutting one: standing the site up, the redirects from the retired ReadTheDocs site, rebuilding `gh-pages` after damage, and removing pre-release versions.

## Setup: bootstrap

The `latest` alias and the `index.html` at the site root that redirects to it would otherwise exist only once a stable release had been deployed, leaving `https://pika.github.io/pika/` and every `latest/` URL returning 404 in the meantime, including the ones compiled into the adapter deprecation warnings of any wheel released before then.

The `main` deploy handles this itself: it asks for `latest` on every push, so the first deploy takes the alias and writes the root redirect, and after a release has taken `latest` the deploy's alias guard declines to move it back. No manual command is needed, and re-running it is safe.

What does need doing by hand, once, is the repository configuration:

1. Confirm no branch-protection rule on `gh-pages` blocks the `github-actions[bot]` push. If the first deploy fails at the push, this is why.
2. Set Settings -> Pages -> Source to `Deploy from a branch`, branch `gh-pages`, folder `/ (root)`. The branch has to exist first, so do this after the first deploy.

## ReadTheDocs redirects

The Sphinx docs on `pika.readthedocs.io` are retired, and years of indexed links and third-party references point at them. RTD redirects forward those to the MkDocs site. This is a one-time setup, recorded here because the next person to touch it will not be the one who did it.

`utils/rtd_redirects.json` holds the mapping and `utils/push_rtd_redirects.py` pushes it. There is no maintained Python client for the RTD API - the `readthedocs` and `readthedocs-client` names on PyPI are reservations with no uploaded files - so the script talks to API v3 over stdlib `urllib`.

```bash
# No token needed: prints every request it would send.
python3 utils/push_rtd_redirects.py

# What RTD holds now, and the diff against the mapping.
python3 utils/push_rtd_redirects.py --list
python3 utils/push_rtd_redirects.py            # with a token: shows the plan

# Send it, then confirm the old URLs land on the new site.
python3 utils/push_rtd_redirects.py --apply
python3 utils/push_rtd_redirects.py --verify
```

The token comes from an RTD account's API tokens page. The script reads `~/.config/rtd-token` or `RTD_TOKEN`; prefer the file, so the value stays out of shell history and process listings. Nothing is committed and nothing authenticated happens without `--apply`.

Five things about the mapping are deliberate and easy to get wrong:

- **The rules are `page` redirects and `from_url` carries no version prefix.** RTD applies a page redirect across every version, so `/intro.html` covers `/en/stable/intro.html`, `/en/latest/intro.html` and `/en/1.3.2/intro.html` in one rule. Writing `/en/stable/intro.html` instead would need an `exact` rule per version and would miss the older versions search engines still hold.
- **`to_url` carries `latest/`.** The new site is versioned by `mike` and has no unversioned page paths: `/pika/intro/` is a 404 and `/pika/latest/intro/` is not. The site root is the one exception, because `/pika/` serves a redirect that follows the `latest` alias and so survives an alias change.
- **`force: true`**, so a rule fires even where the old page still builds, and **`http_status: 301`**, so search engines move their index rather than treating it as temporary.
- **Verify against the deployed site, never `mkdocs serve`.** A local server has no version directories, so every one of these paths resolves locally whether or not it is right in production.
- **Directory roots need rules of their own.** Sphinx writes `index.html` but serves `/en/stable/` and `/en/stable/modules/` as directory indexes, and a rule on `/index.html` does not match `/`. Three such paths existed, and they were still serving the retired docs at 200 after every explicit `.html` path already redirected. That is the shape that hides this: the links a mapping is derived from all work, while the URL a person types does not, and `/en/stable/` is where the bare `readthedocs.io` domain sends its visitors.

Re-running is safe: matching rules are left alone, differing ones are updated in place, and rules on RTD that the mapping does not describe are reported but never deleted, so anything set by hand in the dashboard survives.

`--verify` probes a wildcard rule with an unmapped path rather than with its own `from_url`. Substituting `/en/*` builds `https://pika.readthedocs.io/en/stable/en/*`, which the catch-all then answers, so the check would report success having tested nothing.

Two things confirmed against the live API in October 2026, so they need not be re-derived:

- A `page` redirect accepts an absolute off-site `to_url`. That is the whole basis for one version-agnostic rule per page rather than an `exact` rule per version, and RTD's published documentation does not say so either way. Proven by creating a single rule, confirming the 301, and checking it also covered `/en/latest/` and `/en/1.3.2/` without per-version rules.
- RTD normalises `from_url` by stripping a trailing slash: `/modules/` is stored as `/modules`, while `/` is kept. The script compares normalised paths for that reason. Without it an existing rule reads as missing and `--apply` creates a duplicate on every run, which a dry run shows as "2 to create, 2 on RTD not in the mapping".
- The valid `type` values are `page`, `exact`, `clean_url_to_html` and `html_to_clean_url`, established by posting each candidate and keeping the ones that returned 201. The API does not list the choices when it rejects one, but its rejection messages are informative: `sphinx_html` and `sphinx_htmldir` name their replacements, and `prefix` says "Prefix redirects have been removed. Please use an exact redirect `/prefix/*` instead", which is where the wildcard syntax comes from.
- **The catch-all is the last rule and is an `exact` redirect on `/en/*`.** Its target is fixed rather than a `:splat` passthrough, because the two sites do not share a URL shape: the old paths end in `.html` and the new ones are directories, so passing the tail through would 404 on every page. The API accepts `:splat`, `$rest` and a fixed target alike, so acceptance proves nothing here and only behaviour does.
- **RTD appends a new rule to the end of the list, and lower positions win.** Posting the catch-all with `position: 100` stored it as `position: 21`, after the 21 specific rules, which is what makes `faq.html` still reach `latest/faq/` while an unmapped path reaches the docs home. Verified both ways round.

## Rebuilding `gh-pages` from scratch

`gh-pages` holds built output only, so it can be deleted and regenerated. The history is not worth preserving, but the layout is: `mike` records versions in a `versions.json` that only the deploys themselves write, so a rebuild has to replay each version rather than restore a snapshot.

**Delete the local branch as well as the remote one.** `mike` syncs from the remote only when the local branch is behind it or has diverged; when the local branch is merely *ahead*, which is what a freshly deleted remote makes it, `mike` uses the local branch as-is and republishes the entire pre-delete version index without a warning. Deleting only the remote therefore looks like it worked and undoes the rebuild:

```bash
git push origin --delete gh-pages
git branch -D gh-pages          # the step that is easy to miss
```

Then, for every version to republish, check out the **newest** tag in that series and deploy under the name the original deploy used:

```bash
git checkout <newest tag in the series, e.g. 1.5.3>
hatch run docs:mike deploy --push 1.5    # MAJOR.MINOR, not the full tag
```

The newest tag matters because a stable release publishes `MAJOR.MINOR` and each patch overwrites that same directory. Rebuilding `1.5` from tag `1.5.0` when the series ended at `1.5.3` silently reverts the published `1.5` docs by three patch releases, and nothing records which patch a directory holds.

Then redeploy `dev` from `main` and re-establish the alias and the root redirect, which no `deploy` recreates:

```bash
git checkout main
hatch run docs:mike deploy --push dev
hatch run docs:mike alias --push --update-aliases <newest MAJOR.MINOR> latest
hatch run docs:mike set-default --push latest
```

Deploy order does not matter: `mike` sorts the version selector itself, newest first, with non-version names such as `dev` at the top.

Only versions whose tag carries an `mkdocs.yml` can be replayed, which today means none of them: every released tag up to and including the 1.4 series predates the MkDocs migration. Until 1.5.0 ships there is nothing to replay, and a rebuild reduces to redeploying `dev` from `main` and pointing `latest` at it.

## Pruning pre-release versions

A pre-release publishes a full copy of the site under its own version, and no alias points at it. Nothing removes those, so the version selector accumulates every `b1` and `rc1` indefinitely. Delete them once the stable release they led to has shipped:

```bash
hatch run docs:mike list                        # see what is published
hatch run docs:mike delete --push 1.5.0rc1
```
