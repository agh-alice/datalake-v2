{{/*
datalake-compute chart helpers — intentionally minimal.

Plan 5 Task 1: at that point this tier had no authored templates of its
own; Trino arrived as a chart dependency in Plan 5 Task 2
(envs/prod/compute/Chart.yaml `dependencies:`), and Plan 5 Task 3 added
this chart's first authored template (templates/external-secrets.yaml).
This helpers file itself still defines nothing (no shared naming/label
logic has been needed yet) and renders no manifest on its own.
*/}}
