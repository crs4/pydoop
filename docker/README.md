`Dockerfile.*base` images encapsulate the less frequently changing
parts (prerequisites) of the top-level ones.

| Base file           | Base tag         | Top-level file  | Top-level tag |
|---------------------|------------------|-----------------|---------------|
| Dockerfile.base     | pydoop-base      | Dockerfile      | pydoop        |
| Dockerfile.docsbase | pydoop-docs-base | Dockerfile.docs | pydoop-docs   |

Decoupling the more stable subset into the base images allows for a
faster development cycle. Ideally, the base images should only be
updated when prerequisites need updating:

```
cd docker
docker build -t crs4/pydoop-base -f Dockerfile.base .
docker build -t crs4/pydoop-docs-base -f Dockerfile.docsbase .
docker login
docker push crs4/pydoop-base
docker push crs4/pydoop-docs-base
```

We use no explicit tagging for the base images since we always use the
latest ones.

Base images also make it easier to develop alternate derived images,
e.g., for testing installation via pip.
