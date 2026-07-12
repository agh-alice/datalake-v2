"""Unit tests for alice_ingest.sitesonar's pure/fake-able logic (Plan 2
Task 4, Step 3): listing-HTML parsing and per-file LZMA/JSON-lines decode.

No real network access is used here -- the live-URL e2e probe
(`alice-ingest run-sitesonar --limit 1` against the real
alimonitor.cern.ch endpoint) is a separate, explicit one-shot check
recorded in task-4-report.md, not part of this suite (this VM's outbound
reachability to a third-party CERN host is not something a unit test
should depend on).
"""

from __future__ import annotations

import json
import lzma

import pytest

from alice_ingest.sitesonar import (
    _iter_rows,
    fetch_and_parse,
    list_source_files,
    select_files_to_fetch,
)

# A trimmed real excerpt of the directory listing's HTML shape, verified
# live against http://alimonitor.cern.ch/download/kalana/ (2026-07-12,
# Plan 2 Task 4) -- same <a href="/download/kalana/site-sonar-<epoch>.out.xz">
# structure, alternating row background color, non-.out.xz "Up To" link
# that must NOT be matched.
SAMPLE_LISTING_HTML = """
<!doctype html>
<html lang="en">
<body>
<h1>Directory Listing For [/download/kalana/] – <a href="/download/"><b>Up To [/download]</b></a></h1>
<table>
<tbody>
<tr>
<td align="left">&nbsp;&nbsp;
<a href="/download/kalana/site-sonar-1702260001.out.xz"><tt>site-sonar-1702260001.out.xz</tt></a></td>
<td align="right"><tt>6296.8 KiB</tt></td>
<td align="right"><tt>Mon, 11 Dec 2023 02:00:10 GMT</tt></td>
</tr>
<tr bgcolor="#eeeeee">
<td align="left">&nbsp;&nbsp;
<a href="/download/kalana/site-sonar-1783803541.out.xz"><tt>site-sonar-1783803541.out.xz</tt></a></td>
<td align="right"><tt>5568.3 KiB</tt></td>
<td align="right"><tt>Wed, 08 Jul 2026 20:59:03 GMT</tt></td>
</tr>
<tr>
<td align="left">&nbsp;&nbsp;
<a href="/download/kalana/site-sonar-1743199141.out.xz"><tt>site-sonar-1743199141.out.xz</tt></a></td>
<td align="right"><tt>8630.1 KiB</tt></td>
<td align="right"><tt>Fri, 28 Mar 2025 21:59:04 GMT</tt></td>
</tr>
</tbody>
</table>
</body>
</html>
"""


class TestListSourceFiles:
    def test_extracts_all_out_xz_entries(self):
        files = list_source_files(SAMPLE_LISTING_HTML, "http://alimonitor.cern.ch/download/kalana/")
        assert len(files) == 3

    def test_sorted_most_recent_epoch_first(self):
        files = list_source_files(SAMPLE_LISTING_HTML, "http://alimonitor.cern.ch/download/kalana/")
        epochs = [epoch for epoch, _url in files]
        assert epochs == sorted(epochs, reverse=True)
        assert epochs[0] == 1783803541

    def test_resolves_relative_href_against_base_url(self):
        files = list_source_files(SAMPLE_LISTING_HTML, "http://alimonitor.cern.ch/download/kalana/")
        urls = {url for _epoch, url in files}
        assert "http://alimonitor.cern.ch/download/kalana/site-sonar-1783803541.out.xz" in urls

    def test_does_not_match_the_up_to_navigation_link(self):
        files = list_source_files(SAMPLE_LISTING_HTML, "http://alimonitor.cern.ch/download/kalana/")
        urls = {url for _epoch, url in files}
        assert not any(url.endswith("/download/") for url in urls)

    def test_empty_listing_yields_no_files(self):
        assert list_source_files("<html><body>nothing here</body></html>", "http://x/") == []


class FakeResponse:
    def __init__(self, content: bytes, status: int = 200):
        self.content = content
        self._status = status

    def raise_for_status(self):
        if self._status >= 400:
            raise RuntimeError(f"HTTP {self._status}")


class FakeSession:
    def __init__(self, response: FakeResponse):
        self._response = response
        self.requested_urls = []

    def get(self, url, timeout=None):
        self.requested_urls.append(url)
        return self._response


def _lzma_jsonlines(records: list[dict | None], raw_lines: list[str] | None = None) -> bytes:
    lines = [json.dumps(r) for r in records] if records else []
    if raw_lines:
        lines.extend(raw_lines)
    return lzma.compress("\n".join(lines).encode("utf-8"))


class TestFetchAndParse:
    def test_yields_one_dict_per_json_line(self):
        rows = [
            {"host_id": 1, "hostname": "a.cern.ch", "last_updated": 1783773268},
            {"host_id": 2, "hostname": "b.cern.ch", "last_updated": 1783779448},
        ]
        session = FakeSession(FakeResponse(_lzma_jsonlines(rows)))

        result = list(fetch_and_parse("http://x/site-sonar-1.out.xz", session))

        assert result == rows
        assert session.requested_urls == ["http://x/site-sonar-1.out.xz"]

    def test_skips_malformed_lines_without_raising(self):
        good = {"host_id": 1, "hostname": "a.cern.ch", "last_updated": 1}
        content = _lzma_jsonlines(
            [good], raw_lines=["not json at all {{{", '{"unterminated": '],
        )
        session = FakeSession(FakeResponse(content))

        result = list(fetch_and_parse("http://x/site-sonar-2.out.xz", session))

        assert result == [good]  # both malformed lines skipped, not raised

    def test_skips_blank_lines(self):
        content = lzma.compress(b'{"a": 1}\n\n\n{"a": 2}\n')
        session = FakeSession(FakeResponse(content))

        result = list(fetch_and_parse("http://x/site-sonar-3.out.xz", session))

        assert result == [{"a": 1}, {"a": 2}]

    def test_empty_file_yields_nothing(self):
        session = FakeSession(FakeResponse(lzma.compress(b"")))

        result = list(fetch_and_parse("http://x/site-sonar-4.out.xz", session))

        assert result == []

    def test_preserves_nested_test_results_json_shape(self):
        # test_results_json's sub-keys genuinely vary run to run (module
        # docstring) -- fetch_and_parse must not flatten or drop it; that's
        # dlt's job downstream, under schema_contract columns=evolve.
        row = {
            "host_id": 42,
            "hostname": "wn01.site.alice",
            "last_updated": 1783773268,
            "test_results_json": {"os": {"OS_NAME": "Red Hat", "EXITCODE": 0}},
        }
        session = FakeSession(FakeResponse(_lzma_jsonlines([row])))

        result = list(fetch_and_parse("http://x/site-sonar-5.out.xz", session))

        assert result == [row]
        assert result[0]["test_results_json"]["os"]["OS_NAME"] == "Red Hat"


# ---------------------------------------------------------------------------
# High-water-mark file skip (Plan 2 Task 5 -- scoped in from the P2T4 review:
# already-processed .out.xz files must be SKIPPED, not re-downloaded, on
# subsequent runs). TDD: list-in -> fetch-list-out given a high-water mark.
# ---------------------------------------------------------------------------


class TestSelectFilesToFetch:
    """Pure filter: files whose filename-embedded epoch is already <= the
    high-water mark have already been fetched by a prior run and must be
    excluded. Strictly-greater-than: a file AT the high-water mark was the
    last one fetched, not a new one."""

    def test_initial_high_water_mark_of_zero_selects_every_file(self):
        files = [(300, "u3"), (200, "u2"), (100, "u1")]

        assert select_files_to_fetch(files, high_water_mark=0) == files

    def test_only_files_strictly_newer_than_the_high_water_mark_are_selected(self):
        files = [(300, "u3"), (200, "u2"), (100, "u1")]

        result = select_files_to_fetch(files, high_water_mark=200)

        assert result == [(300, "u3")]

    def test_file_exactly_at_the_high_water_mark_is_excluded_not_reselected(self):
        files = [(200, "u2"), (100, "u1")]

        assert select_files_to_fetch(files, high_water_mark=200) == []

    def test_high_water_mark_at_or_above_every_epoch_selects_nothing(self):
        files = [(300, "u3"), (200, "u2")]

        assert select_files_to_fetch(files, high_water_mark=999) == []

    def test_preserves_the_most_recent_first_ordering(self):
        files = [(300, "u3"), (250, "u25"), (200, "u2")]

        result = select_files_to_fetch(files, high_water_mark=150)

        assert result == files  # order untouched, all three qualify

    def test_empty_file_list_selects_nothing(self):
        assert select_files_to_fetch([], high_water_mark=0) == []


class FakeMultiUrlSession:
    """Unlike FakeSession (one canned response for any URL), this fake
    routes .get() by URL -- needed to simulate the listing page (base_url)
    and N distinct per-file .out.xz responses in the same test."""

    def __init__(self, responses: dict[str, "FakeResponse"]):
        self._responses = responses
        self.requested_urls: list[str] = []

    def get(self, url, timeout=None):
        self.requested_urls.append(url)
        return self._responses[url]


BASE_URL = "http://alimonitor.cern.ch/download/kalana/"


def _listing_html(epochs: list[int]) -> str:
    rows = "\n".join(
        f'<a href="/download/kalana/site-sonar-{epoch}.out.xz">'
        f"<tt>site-sonar-{epoch}.out.xz</tt></a>"
        for epoch in epochs
    )
    return f"<html><body><table><tbody>{rows}</tbody></table></body></html>"


def _file_response(epoch: int, host_suffix: str = "") -> "FakeResponse":
    row = {"host_id": epoch, "hostname": f"h{epoch}{host_suffix}.cern.ch", "last_updated": epoch}
    return FakeResponse(_lzma_jsonlines([row]))


class TestIterRowsHighWaterMark:
    """`_iter_rows` wires `select_files_to_fetch` against a caller-supplied
    `state` dict (production: `dlt.current.resource_state()`, per
    sitesonar.py's module docstring; a plain dict here -- same fake-able
    seam convention as retention.py's Protocol-based catalog fakes) and
    advances `state["high_water_mark"]` after a successful pass over
    whichever files were selected."""

    def test_first_run_with_empty_state_fetches_every_listed_file(self):
        listing_url = BASE_URL
        file_100 = f"{BASE_URL}site-sonar-100.out.xz"
        file_200 = f"{BASE_URL}site-sonar-200.out.xz"
        session = FakeMultiUrlSession(
            {
                listing_url: FakeResponse(_listing_html([100, 200]).encode()),
                file_100: _file_response(100),
                file_200: _file_response(200),
            }
        )
        # listing response must be .text-capable (requests.Response API);
        # FakeResponse only has .content -- patch a .text attr for this test.
        session._responses[listing_url].text = _listing_html([100, 200])

        state: dict = {}
        rows = list(_iter_rows(BASE_URL, None, session, state))

        assert {r["host_id"] for r in rows} == {100, 200}
        assert state["high_water_mark"] == 200

    def test_second_run_with_state_at_max_epoch_downloads_nothing(self):
        listing_url = BASE_URL
        session = FakeMultiUrlSession(
            {listing_url: FakeResponse(b"", status=200)},
        )
        session._responses[listing_url].text = _listing_html([100, 200])

        state = {"high_water_mark": 200}
        rows = list(_iter_rows(BASE_URL, None, session, state))

        assert rows == []
        # Only the listing page itself was requested -- no per-file .out.xz
        # download happened (the skip -- proves files are not re-fetched).
        assert session.requested_urls == [listing_url]
        assert state["high_water_mark"] == 200  # unchanged, nothing new fetched

    def test_partial_run_only_fetches_files_newer_than_the_high_water_mark(self):
        listing_url = BASE_URL
        file_300 = f"{BASE_URL}site-sonar-300.out.xz"
        session = FakeMultiUrlSession(
            {
                listing_url: FakeResponse(b""),
                file_300: _file_response(300),
            }
        )
        session._responses[listing_url].text = _listing_html([300, 200, 100])

        state = {"high_water_mark": 200}
        rows = list(_iter_rows(BASE_URL, None, session, state))

        assert {r["host_id"] for r in rows} == {300}
        assert session.requested_urls == [listing_url, file_300]
        assert state["high_water_mark"] == 300

    def test_limit_caps_the_new_files_not_the_full_listing(self):
        # Two files are new (300, 250); --limit 1 must fetch only the more
        # recent of the NEW ones, not just "the first file in the listing".
        listing_url = BASE_URL
        file_300 = f"{BASE_URL}site-sonar-300.out.xz"
        session = FakeMultiUrlSession(
            {
                listing_url: FakeResponse(b""),
                file_300: _file_response(300),
            }
        )
        session._responses[listing_url].text = _listing_html([300, 250, 200])

        state = {"high_water_mark": 200}
        rows = list(_iter_rows(BASE_URL, limit=1, session=session, state=state))

        assert {r["host_id"] for r in rows} == {300}
        assert file_300 in session.requested_urls
        assert state["high_water_mark"] == 300

    def test_a_run_that_fetches_nothing_new_leaves_state_absent_key_untouched(self):
        # No prior state key at all (very first run of a brand new
        # pipeline) combined with an empty listing -- must not crash on
        # state.get() and must not fabricate a high_water_mark of 0.
        listing_url = BASE_URL
        session = FakeMultiUrlSession({listing_url: FakeResponse(b"")})
        session._responses[listing_url].text = "<html><body>empty</body></html>"

        state: dict = {}
        rows = list(_iter_rows(BASE_URL, None, session, state))

        assert rows == []
        assert "high_water_mark" not in state

    def test_logs_skip_evidence(self, capsys):
        listing_url = BASE_URL
        session = FakeMultiUrlSession({listing_url: FakeResponse(b"")})
        session._responses[listing_url].text = _listing_html([100, 200])

        state = {"high_water_mark": 200}
        list(_iter_rows(BASE_URL, None, session, state))

        out = capsys.readouterr().out
        assert "SITESONAR" in out
        assert "high_water_mark=200" in out
        assert "files_already_processed_skipped=2" in out
        assert "files_to_fetch=0" in out


class TestIterRowsPerFileProgressLogging:
    """N5-part (final-review, minor): a long site-sonar backlog run (the
    real cyfronet case -- 1740+ files, unbounded, module docstring) prints
    nothing between the initial `SITESONAR ... files_to_fetch=N` summary
    line and completion, giving an operator watching pod logs no signal of
    progress for a run that can take a long time. One `n/total` progress
    line per file fetched, interleaved with the actual fetch/yield loop."""

    def test_logs_one_progress_line_per_file_with_running_count(self, capsys):
        listing_url = BASE_URL
        file_100 = f"{BASE_URL}site-sonar-100.out.xz"
        file_200 = f"{BASE_URL}site-sonar-200.out.xz"
        session = FakeMultiUrlSession(
            {
                listing_url: FakeResponse(_listing_html([100, 200]).encode()),
                file_100: _file_response(100),
                file_200: _file_response(200),
            }
        )
        session._responses[listing_url].text = _listing_html([100, 200])

        state: dict = {}
        list(_iter_rows(BASE_URL, None, session, state))

        out = capsys.readouterr().out
        assert "SITESONAR file 1/2" in out
        assert "SITESONAR file 2/2" in out

    def test_progress_lines_respect_the_limit_not_the_full_listing(self, capsys):
        listing_url = BASE_URL
        file_300 = f"{BASE_URL}site-sonar-300.out.xz"
        session = FakeMultiUrlSession(
            {listing_url: FakeResponse(b""), file_300: _file_response(300)}
        )
        session._responses[listing_url].text = _listing_html([300, 250, 200])

        state = {"high_water_mark": 200}
        list(_iter_rows(BASE_URL, limit=1, session=session, state=state))

        out = capsys.readouterr().out
        assert "SITESONAR file 1/1" in out
        assert "2/2" not in out  # only the one limited file, not both new ones

    def test_no_progress_lines_when_nothing_new_to_fetch(self, capsys):
        listing_url = BASE_URL
        session = FakeMultiUrlSession({listing_url: FakeResponse(b"")})
        session._responses[listing_url].text = _listing_html([100, 200])

        state = {"high_water_mark": 200}
        list(_iter_rows(BASE_URL, None, session, state))

        out = capsys.readouterr().out
        assert "SITESONAR file" not in out


# ---------------------------------------------------------------------------
# SITESONAR_LIMIT env var (final-review N5-part): unifies with the
# pre-existing --limit CLI flag -- --limit always wins when given
# explicitly; SITESONAR_LIMIT is the fallback (kind sets it via the
# ingest-env Secret to bound nightly site-sonar runs on the small kind
# cluster; cyfronet leaves it unset for the real, unbounded backlog).
# ---------------------------------------------------------------------------


class TestResolveLimit:
    def test_explicit_cli_limit_wins_over_env(self):
        from alice_ingest.sitesonar import _resolve_limit

        assert _resolve_limit(5, {"SITESONAR_LIMIT": "1"}) == 5

    def test_falls_back_to_env_var_when_cli_limit_is_none(self):
        from alice_ingest.sitesonar import _resolve_limit

        assert _resolve_limit(None, {"SITESONAR_LIMIT": "5"}) == 5

    def test_none_when_neither_cli_nor_env_set(self):
        from alice_ingest.sitesonar import _resolve_limit

        assert _resolve_limit(None, {}) is None

    def test_none_when_env_var_is_empty_string(self):
        from alice_ingest.sitesonar import _resolve_limit

        assert _resolve_limit(None, {"SITESONAR_LIMIT": ""}) is None

    def test_rejects_non_integer_env_value(self):
        from alice_ingest.sitesonar import _resolve_limit

        with pytest.raises(SystemExit, match="SITESONAR_LIMIT"):
            _resolve_limit(None, {"SITESONAR_LIMIT": "not-an-int"})
