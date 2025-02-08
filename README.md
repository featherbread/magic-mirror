# Magic Mirror

Magic Mirror is an **experimental** and **rough** tool to efficiently copy large
sets of container images between registries.

Unlike tools that work with single images at a time, Magic Mirror is designed to
leverage fundamental properties of container images and common registry features
to deduplicate work and minimize data transfers, especially when some layers are
shared between images or already exist in the target registry. It can also
perform some lightweight transformations during the copy process, such as
mirroring only a subset of the platforms in a multi-platform source image.

**Magic Mirror is not actively maintained, and not considered appropriate for
production use.** While it is not known to have correctness issues (e.g.
corrupting image contents), it is known to have implementation deficiencies that
limit its effectiveness for some use cases (for example, treating HTTP 429
responses as hard errors rather than backing off and retrying). It is provided
in the hope that it may be useful as a reference or basis for other work, but is
mostly intended as a playground for my personal learning and exploration with
Go, concurrency patterns, and container image registries.

## Usage

### Copy Specs

Magic Mirror is designed as a flexible _backend_ for an image mirroring process,
rather than a user-friendly frontend to be used directly. It reads a sequence of
JSON-encoded **copy specs** from standard input or a file, each of which
requests the copy of a specific source image to a specific destination.

The input must contain a sequence of copy specs, arrays of copy specs, or both.
Each copy spec must respect the following schema:

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

The `specs.json` file in this repository is an example of a valid input that
could be provided as-is to the `magic-mirror` CLI. However, **it is expected
that you will _generate_ specs** from another data source rather than write them
by hand. For example, you might write a script that pipes `skopeo list-tags`
into a `jq` filter, or you might list images and tags in a custom YAML or TOML
schema and add a hardcoded `limitPlatforms` transform to every spec.

### Validation

To avoid undefined or confusing behavior, the input to a single Magic Mirror run
must satisfy these requirements:

- A single image reference must not appear as both a source and a destination.
- Copy specs can be duplicated, but all copy specs for a given destination image
  must be equivalent, including the full source reference and transformations.
- When a destination reference includes a `sha256:…` digest, the source
  reference must explicitly specify that same digest.
- A copy spec that includes transformations must not include a `sha256:…` digest
  in the destination reference.

### Registry Authentication

Magic Mirror authenticates to registries using credentials set by `docker login`
or `podman login`. [See the go-containerregistry documentation][authn docs]
for information about how credentials are found and the format of relevant
config files.

Note that if your configuration uses Docker credential helpers, you may need to
satisfy their requirements to run Magic Mirror. For example, if you use Docker
Desktop, you may need to have it running to authenticate with private registries
even though Magic Mirror will not touch your Docker daemon in any way.

[authn docs]: https://pkg.go.dev/github.com/google/go-containerregistry@v0.13.0/pkg/authn#section-readme

## How It Works

To fully understand what Magic Mirror is doing (and in particular the progress
statistics that it reports every few seconds), it helps to understand how
container images and registries work under the hood. When you `docker push` an
image to a registry, the registry receives two different kinds of objects:

- **Blobs** can be any arbitrary file, like a `.tar.gz` of an image layer or a
  JSON file containing default entrypoint and environment settings. A key
  feature of blobs is that you can only download them from a registry when you
  know the hash of their content (typically SHA-256) in advance.
- **Manifests** are relatively small JSON files listing the hashes of all of the
  blobs (filesystem layers and configuration data) that make up a single image.
  Multi-platform images use a special **index** (or "manifest list") format that
  instead lists the hashes of the individual manifests for different platforms.
  Unlike blobs, manifests can be stored and downloaded using human-readable tags
  in addition to their SHA-256 hashes.

Later on, to `docker pull` an image, the Docker daemon will:

1. Download the **manifest** matching your requested tag.
2. Check if the manifest is actually an **index**, and if so download the
   underlying manifest for the current platform.
3. Download the image configuration and filesystem layers in parallel using the
   **blob** hashes from the manifest.

Since Magic Mirror works with sets of images instead of a single image at a
time, it can work on several parts of a large transfer concurrently, including:

- Downloading image manifests from source registries to determine which blobs
  will need to exist in the destination.
- Downloading image manifests from destination registries, both to compare them
  with the source manifests to see if the destination is already up to date, and
  to discover which blobs already exist at the destination.
- Transferring blobs that a destination repository doesn't already have, either
  by copying over the network from the source, or by asking the registry to copy
  blobs that Magic Mirror knows it already has for other images (which will be
  faster and save network bandwidth).
- Writing manifests to destination registries as soon as the required blobs are
  known to exist there.

When Magic Mirror starts a copy operation, you may see the total blob and
platform counts in the progress output increase as it discovers the specific
blobs and manifests for the source images, and eventually level off. Even though
Magic Mirror can have copies for many different images in flight at the same
time, it will try to group blobs and platforms together to increase the number
of fully copied images if it gets interrupted.
