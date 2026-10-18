#!/usr/bin/env python3
"""Verify the one immutable workspace-base mirror; never publish or repair it."""

import base64
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


DIGEST = "sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437"
PACKAGE = "swarm-workspace-golang"
REPOSITORY = "division-sh/swarm"
SOURCE = "https://public.ecr.aws/v2/docker/library/golang"
MIRROR = "https://ghcr.io/v2/division-sh/" + PACKAGE
ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.artifact.manifest.v1+json",
])


class BlobRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        if urllib.parse.urlsplit(new_url).scheme != "https":
            raise ValueError("non-HTTPS registry redirect refused")
        redirected = super().redirect_request(request, response, code, message, headers, new_url)
        if urllib.parse.urlsplit(new_url).netloc != urllib.parse.urlsplit(request.full_url).netloc:
            redirected.remove_header("Authorization")
        return redirected


def read_json(opener, url, authorization):
    request = urllib.request.Request(url, headers={"Authorization": authorization, "Accept": "application/json"})
    with opener.open(request, timeout=60) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024:
        raise ValueError("oversized setup metadata")
    return json.loads(raw)


def descriptor(value):
    digest, size = value.get("digest"), value.get("size")
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("invalid fixed-image descriptor digest")
    if type(size) is not int or size < 0:
        raise ValueError("invalid fixed-image descriptor size")
    return digest, size


def read_manifest(opener, registry, token, digest, size):
    request = urllib.request.Request(registry + "/manifests/" + digest,
                                     headers={"Authorization": "Bearer " + token, "Accept": ACCEPT})
    with opener.open(request, timeout=60) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024 or (size is not None and len(raw) != size):
        raise ValueError("manifest size differs from immutable descriptor")
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("manifest bytes differ from immutable digest")
    return raw


def verify_blob(opener, token, digest, size):
    request = urllib.request.Request(MIRROR + "/blobs/" + digest, headers={"Authorization": "Bearer " + token})
    actual, count = hashlib.sha256(), 0
    with opener.open(request, timeout=60) as response:
        while chunk := response.read(1024 * 1024):
            count += len(chunk)
            if count > size:
                raise ValueError("blob exceeds immutable descriptor size")
            actual.update(chunk)
    if count != size or "sha256:" + actual.hexdigest() != digest:
        raise ValueError("blob bytes differ from immutable descriptor")


def linked_package(opener, token, allow_absent=False):
    try:
        package = read_json(opener, "https://api.github.com/orgs/division-sh/packages/container/" + PACKAGE, "Bearer " + token)
    except urllib.error.HTTPError as error:
        if allow_absent and error.code == 404:
            return None
        raise
    repository = package.get("repository") or {}
    if (package.get("name") != PACKAGE or package.get("package_type") != "container"
            or package.get("visibility") != "private" or repository.get("full_name") != REPOSITORY
            or type(package.get("id")) is not int or package["id"] <= 0
            or repository.get("id") != int(os.environ["GITHUB_REPOSITORY_ID"])):
        raise ValueError("private package/repository association refused; user policy action required")
    return package


def verify():
    token, actor = os.environ["GH_TOKEN"], os.environ["GITHUB_ACTOR"]
    opener = urllib.request.build_opener(BlobRedirect())
    package = linked_package(opener, token)
    source_token = read_json(opener, "https://public.ecr.aws/token/?service=public.ecr.aws&scope=repository:docker/library/golang:pull", "")["token"]
    basic = base64.b64encode((actor + ":" + token).encode()).decode()
    mirror_token = read_json(opener, "https://ghcr.io/token?service=ghcr.io&scope=repository:division-sh/" + PACKAGE + ":pull", "Basic " + basic)["token"]
    pending, manifests, blobs = [(DIGEST, None)], {}, {}
    while pending:
        digest, size = pending.pop()
        if digest in manifests:
            if size is not None and manifests[digest] != size:
                raise ValueError("crossed manifest size")
            continue
        if len(manifests) >= 128:
            raise ValueError("fixed-image manifest graph limit exceeded")
        source = read_manifest(opener, SOURCE, source_token, digest, size)
        mirror = read_manifest(opener, MIRROR, mirror_token, digest, size)
        if source != mirror:
            raise ValueError("source/mirror manifest bytes differ")
        manifests[digest] = len(source)
        manifest = json.loads(source)
        if manifest.get("schemaVersion") != 2:
            raise ValueError("unknown fixed-image manifest schema")
        if "manifests" in manifest:
            pending.extend(descriptor(item) for item in manifest["manifests"])
        elif "config" in manifest and "layers" in manifest:
            for item in [manifest["config"], *manifest["layers"]]:
                blob, length = descriptor(item)
                if blob in blobs and blobs[blob] != length:
                    raise ValueError("crossed blob size")
                blobs[blob] = length
        elif "blobs" in manifest:
            for item in manifest["blobs"]:
                blob, length = descriptor(item)
                if blob in blobs and blobs[blob] != length:
                    raise ValueError("crossed auxiliary blob size")
                blobs[blob] = length
        else:
            raise ValueError("unknown fixed-image manifest form")
        if "subject" in manifest:
            pending.append(descriptor(manifest["subject"]))
        if len(blobs) > 4096:
            raise ValueError("fixed-image blob graph limit exceeded")
    for digest, size in sorted(blobs.items()):
        verify_blob(opener, mirror_token, digest, size)
    return {"repository": REPOSITORY, "package": PACKAGE, "visibility": "private", "digest": DIGEST,
            "package_id": package["id"], "repository_id": package["repository"]["id"],
            "source_head": os.environ["GITHUB_SHA"], "manifests": manifests, "verified_blobs": blobs,
            "qualification": False}


if __name__ == "__main__":
    try:
        if sys.argv[1:] in (["--before-publication"], ["--association-only"]):
            package = linked_package(urllib.request.build_opener(BlobRedirect()), os.environ["GH_TOKEN"], sys.argv[1:] == ["--before-publication"])
            print(json.dumps({"package": package, "qualification": False}, sort_keys=True))
        elif sys.argv[1:]:
            raise ValueError("unknown fixed workspace verification mode")
        else:
            print(json.dumps(verify(), sort_keys=True))
    except urllib.error.HTTPError as error:
        print("workspace mirror HTTP refusal " + str(error.code) + "; stop for access/policy disposition", file=sys.stderr)
        sys.exit(1)
    except (KeyError, ValueError, urllib.error.URLError) as error:
        print("workspace mirror verification refused: " + str(error), file=sys.stderr)
        sys.exit(1)
