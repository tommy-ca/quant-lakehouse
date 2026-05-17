from pathlib import Path


def test_no_unapproved_legacy_imports():
    """Ensure imports from `workflow.legacy` appear only in the approved files.

    This protects against accidental reintroduction of legacy imports across the
    codebase. The allowed set is intentionally small and part of the Phase 42
    consolidation plan.
    """
    root = Path.cwd()
    matches = []
    for p in root.rglob("*.py"):
        try:
            text = p.read_text(encoding="utf-8")
        except Exception:
            continue
        # ignore this test file itself
        if p.name == Path(__file__).name:
            continue
        if "workflow.legacy" in text:
            # store relative paths for readable assertions
            matches.append(str(p.relative_to(root)))

    # Whitelist of files that are allowed to reference the legacy helpers.
    allowed = {
        "src/binance_datatool/workflow/gap_fill.py",
        "src/binance_datatool/workflow/sink.py",
        "src/binance_datatool/workflow/prefect_flows.py",
        "docs/proposals/test_lineage.py",
    }

    unexpected = set(matches) - allowed
    assert not unexpected, f"Unexpected workflow.legacy imports in: {sorted(unexpected)}"
