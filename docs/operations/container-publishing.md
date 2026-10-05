# Container Publishing

Cursus publishes its broker container image to GitHub Container Registry (GHCR) through `.github/workflows/docker-publish.yml`.

## Published Image

The upstream image is:

```text
ghcr.io/cursus-io/cursus
```

Each published tag is a manifest list containing `linux/amd64` and
`linux/arm64` images built from the repository `Dockerfile`.

## Publication Triggers And Tags

The publishing workflow runs on:

- pushes to `main`;
- tags matching `v*`; and
- manual `workflow_dispatch` runs.

`docker/metadata-action` generates the published tags:

| Source | Tags |
|---|---|
| `main` push | `main`, `latest`, and `sha-<short-sha>` |
| Semantic version tag such as `v0.2.0` | `0.2.0`, `0.2`, `latest`, and `sha-<short-sha>` |
| Manual run | Ref and SHA tags allowed by the metadata rules |

## Workflow And Permissions

The workflow:

1. checks out the repository without persisting credentials;
2. configures QEMU and Docker Buildx;
3. authenticates to `ghcr.io` with the workflow-scoped `GITHUB_TOKEN`;
4. generates OCI labels and tags; and
5. builds and publishes the amd64 and arm64 manifest list.

The job uses read-only repository contents permission and `packages: write`. The repository organization owns the package. Package visibility and anonymous pull access are managed separately in the GitHub package settings.

## Release Scope

The repository currently publishes container images only. It does not create GitHub Releases, cross-compile binary archives, or upload checksums.

## Verification

After a successful workflow run:

```bash
docker pull ghcr.io/cursus-io/cursus:latest
docker inspect ghcr.io/cursus-io/cursus:latest
```

For a version tag, replace `latest` with the normalized semantic version, for example `0.2.0`.
