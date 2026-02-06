# Docker Usage

Floe is published to GitHub Container Registry (GHCR).

## Pull

```bash
docker pull ghcr.io/malon64/floe:latest
```

## Run

Mount your working directory to `/work` and pass the config path:

```bash
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/example/config.yml
```

## Tips

- Use absolute paths inside the container.
- For cloud storage, pass credentials via environment variables.
- All CLI flags are the same as local usage.
