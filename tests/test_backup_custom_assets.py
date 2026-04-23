import tarfile
from pathlib import Path

from scripts.backup_custom_assets import (
    MANIFEST_SNAPSHOT_NAME,
    METADATA_NAME,
    apply_restore,
    build_restore_plan,
    create_backup,
    resolve_manifest_files,
)


def test_resolve_manifest_files_supports_globs(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src" / "models").mkdir(parents=True)
    (repo / "src" / "models" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "docs").mkdir()
    (repo / "docs" / "guide.md").write_text("# guide\n", encoding="utf-8")
    manifest = repo / "manifest.json"
    manifest.write_text(
        '{"include":["src/models/**/*.py","docs/guide.md","missing/*.txt"]}',
        encoding="utf-8",
    )

    files = resolve_manifest_files(repo, manifest)

    assert [path.relative_to(repo).as_posix() for path in files] == [
        "docs/guide.md",
        "src/models/a.py",
    ]


def test_create_backup_writes_payload_and_metadata(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "strategies").mkdir()
    (repo / "strategies" / "alpha.yaml").write_text("name: alpha\n", encoding="utf-8")
    manifest = repo / "manifest.json"
    manifest.write_text('{"include":["strategies/*.yaml"]}', encoding="utf-8")
    archive = tmp_path / "backup.tar.gz"

    files = create_backup(repo, manifest, archive)

    assert files == ["strategies/alpha.yaml"]
    with tarfile.open(archive, "r:gz") as bundle:
        names = bundle.getnames()
    assert "payload/strategies/alpha.yaml" in names
    assert METADATA_NAME in names
    assert MANIFEST_SNAPSHOT_NAME in names


def test_build_restore_plan_detects_changes_and_new_files(tmp_path: Path) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    (source_repo / "src").mkdir()
    (source_repo / "src" / "feature.py").write_text("print('backup')\n", encoding="utf-8")
    manifest = source_repo / "manifest.json"
    manifest.write_text('{"include":["src/*.py"]}', encoding="utf-8")
    archive = tmp_path / "backup.tar.gz"
    create_backup(source_repo, manifest, archive)

    target_repo = tmp_path / "target"
    target_repo.mkdir()
    (target_repo / "src").mkdir()
    (target_repo / "src" / "feature.py").write_text("print('local change')\n", encoding="utf-8")

    plan = build_restore_plan(archive, target_repo)

    assert plan.changed_files == ["src/feature.py"]
    assert plan.new_files == []
    assert plan.unchanged_files == []


def test_apply_restore_requires_overwrite_for_changed_files(tmp_path: Path) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    (source_repo / "docs").mkdir()
    (source_repo / "docs" / "note.md").write_text("backup\n", encoding="utf-8")
    manifest = source_repo / "manifest.json"
    manifest.write_text('{"include":["docs/*.md"]}', encoding="utf-8")
    archive = tmp_path / "backup.tar.gz"
    create_backup(source_repo, manifest, archive)

    target_repo = tmp_path / "target"
    target_repo.mkdir()
    (target_repo / "docs").mkdir()
    note_path = target_repo / "docs" / "note.md"
    note_path.write_text("different\n", encoding="utf-8")

    try:
        apply_restore(archive, target_repo, overwrite=False)
    except ValueError as exc:
        assert "overwrite" in str(exc)
    else:
        raise AssertionError("expected restore to require explicit overwrite")

    apply_restore(archive, target_repo, overwrite=True)
    assert note_path.read_text(encoding="utf-8") == "backup\n"
