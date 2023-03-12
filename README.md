# Magic Mirror

Magic Mirror is an **experimental** and **rough** tool to efficiently copy large
sets of container images between registries.

Unlike tools that work with single images at a time, Magic Mirror is designed to
leverage the fundamental properties of container images and the features of
typical registries to deduplicate work and minimize data transfers, especially
when the images in a copy set share some of their layers or have only had some
layers changed since the last copy. It can also perform some lightweight
transformations during the copy process, such as mirroring only a subset of the
platforms in a multi-platform source image.

## Usage

### Copy Specs

Magic Mirror is designed as a relatively flexible and unopinionated _backend_
for an image mirroring process, and not so much as a user-friendly frontend to
be used directly. It works from a sequence of JSON-encoded **copy specs** piped
from standard input or read from a file, each of which represents a copy from a
single source image to a single destination image.

Copy specs can be provided as a sequence of JSON objects or arrays of objects
including the following keys:

- **`src`** (string, required): The name of an image just as you would provide
  to `docker pull`. For example, `alpine` corresponds to the `latest` tag of the
  official Alpine Linux image from Docker Hub.
- **`dst`** (string, required): The name of an image just as you would provide
  to `docker push`.
- **`transform`** (object): Optional transformations to perform while
  mirroring an image, rather than copying the entire image exactly as-is.
  - **`limitPlatforms`** (list of strings): A list of platforms just as you
    would provide for Docker's `--platform` flag (for example, `linux/arm/v7`
    for the 32-bit ARMv7 architecture). When non-empty, and when `src` is a
    multi-platform image, only the listed platforms will be copied (rather than
    all platforms found in the image). If the image does not contain any of the
    requested platforms, the copy will fail. When `src` is a single-platform
    image, this option is ignored and the image is copied as-is.

The `specs.json` file in this repository is an example of a fully valid input
that could be provided as-is to the `magic-mirror` CLI. However, **it is
expected that you will generate specs in some kind of script** using upstream
data, rather than writing them out by hand like this. For example, you might use
`skopeo list-tags` to dynamically list the valid tags in a source repository, or
you might define images and tags in a custom YAML or TOML schema and add a
hardcoded `limitPlatforms` transform to every spec you generate.

### Validation

To ensure the consistency of a copy operation, Magic Mirror imposes a few
requirements on the overall set of copy specs provided to a single run:

- A single image reference cannot appear as both a source and a destination.
- Copy specs can be duplicated, but all copy specs for a given destination image
  must be equivalent, including the full source reference and transformations.
- When a `sha256:…` digest is specified on a destination reference, the source
  reference must explicitly specify that same digest.
- A `sha256:…` digest cannot be included in the destination reference of a copy
  spec that includes transformations.

### Registry Authentication

Magic Mirror authenticates to registries when necessary using credentials set by
`docker login` or `podman login`. [See the go-containerregistry documentation][authn docs]
for more information about where credentials are found and the format of the
relevant config files.

Note that if your configuration uses Docker credential helpers, you will need to
ensure that any requirements for using those helpers are met before running
Magic Mirror. For example, if you have Docker Desktop installed and are using
its default credential helper, you may need to have Docker Desktop running to
use Magic Mirror with private registries, even though Magic Mirror does not
touch the Docker daemon in any way.

[authn docs]: https://pkg.go.dev/github.com/google/go-containerregistry@v0.13.0/pkg/authn#section-readme
