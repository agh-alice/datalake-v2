{{/*
datalake-compute chart helpers — intentionally minimal.

Plan 5 Task 1: this tier has no authored templates of its own yet. Trino
arrives as a chart dependency in Plan 5 Task 2 (envs/prod/compute/Chart.yaml
`dependencies:`), which is what will actually populate this chart's
rendered output. Until then, `helm template envs/prod/compute` renders
nothing beyond this file's comment (which itself produces no manifest --
Helm skips template files whose output is empty), satisfying the "renders
to empty output cleanly" requirement (Task 1 brief).
*/}}
