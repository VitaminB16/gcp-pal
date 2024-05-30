from uuid import uuid4
from gcp_pal import ArtifactRegistry


def test_constructor():
    success = {}

    success[0] = ArtifactRegistry().level == "location"
    success[1] = ArtifactRegistry(location=None).level == "project"
    success[2] = ArtifactRegistry("gcr.io").level == "repository"
    success[3] = ArtifactRegistry("gcr.io/image-name").level == "image"
    success[4] = ArtifactRegistry("gcr.io/image-name/sha256:version").level == "version"
    success[5] = ArtifactRegistry("gcr.io/image-name:tag").level == "tag"
    success[6] = ArtifactRegistry("gcr.io").location == "europe-west2"
    success[7] = ArtifactRegistry("gcr.io", location="us").location == "us"
    success[8] = ArtifactRegistry("projects/my-project").project == "my-project"
    success[9] = (
        ArtifactRegistry("projects/my-project/locations/us-central1").location
        == "us-central1"
    )
    success[10] = (
        ArtifactRegistry(
            "projects/my-project/locations/us-central1/repositories/my-repo"
        ).repository
        == "my-repo"
    )
    success[11] = (
        ArtifactRegistry(
            "projects/my-project/locations/us-central1/repositories/my-repo/images/my-image"
        ).image
        == "my-image"
    )
    success[12] = ArtifactRegistry("gcr.io/image-name:tag").tag == "tag"
    success[13] = (
        ArtifactRegistry("gcr.io/image-name/sha256:version").version == "version"
    )
    success[14] = (
        ArtifactRegistry("gcr.io/image-name/sha256:version").image == "image-name"
    )
    success[15] = (
        ArtifactRegistry("gcr.io/image-name/sha256:version").repository == "gcr.io"
    )
    r = ArtifactRegistry(repository="gcr.io", location="us")
    success[16] = r.image is None
    success[17] = r.repository == "gcr.io"
    success[18] = r.location == "us"
    success[19] = r.level == "repository"
    r = ArtifactRegistry(repository="gcr.io", location="us", image="my-image")
    success[16] = r.image == "my-image"
    success[17] = r.repository == "gcr.io"
    success[18] = r.location == "us"
    success[19] = r.level == "image"

    failed = [k for k, v in success.items() if not v]

    assert not failed


def test_create_repository():
    success = {}

    repo_name = f"test-repository-{uuid4().hex[:10]}"

    success[0] = not ArtifactRegistry(repo_name).exists()
    success[1] = repo_name not in ArtifactRegistry().ls()

    ArtifactRegistry(repo_name).create_repository()

    success[2] = ArtifactRegistry(repo_name).exists()
    success[3] = repo_name in ArtifactRegistry().ls()

    ArtifactRegistry(repo_name).delete()

    success[4] = not ArtifactRegistry(repo_name).exists()
    success[5] = repo_name not in ArtifactRegistry().ls()

    failed = [k for k, v in success.items() if not v]

    assert not failed
