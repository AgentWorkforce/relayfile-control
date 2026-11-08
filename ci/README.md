# CI

`github-actions-ci.yml` is the CI workflow for this repo (typecheck, build,
the full test suite against a Postgres 17 service, and a Docker build).

It is not under `.github/workflows/` yet: the implementing agent's GitHub
token has no `workflow` scope and cannot push workflow files. A maintainer
installs it with:

```sh
mkdir -p .github/workflows && git mv ci/github-actions-ci.yml .github/workflows/ci.yml
```
