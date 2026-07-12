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

from alice_ingest.sitesonar import fetch_and_parse, list_source_files

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
