{{/*
Name helpers. Nothing here is Remount-specific; they exist so a release can be
installed more than once in a cluster without colliding.
*/}}
{{- define "remount-node.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "remount-node.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "remount-node.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "remount-node.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: remount-node
{{- end -}}

{{- define "remount-node.selectorLabels" -}}
app.kubernetes.io/name: {{ include "remount-node.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "remount-node.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "remount-node.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The image reference, pinned by digest.

`required` here is the whole point: a chart that silently rendered a floating
tag would let the image that was reviewed and the image that runs diverge, and
nothing in the cluster would show it. Rendering fails instead.
*/}}
{{- define "remount-node.image" -}}
{{- $digest := required "image.digest is required: the Remount node image must be pinned by digest, never by a tag" .Values.image.digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "image.digest must be sha256:<64 hex>, got %q" $digest) -}}
{{- end -}}
{{- $repository := required "image.repository is required" .Values.image.repository -}}
{{- /*
Refuse the control-plane image. packaging/container/Dockerfile is FROM scratch,
which is right for the control plane and the CLI and impossible for a
process-backend node: the workspace has no userland, so the first session dies
with `"sh": executable file not found in $PATH`. The failure looks like a
Remount bug and is a packaging mistake, so the chart refuses it up front.
*/ -}}
{{- if regexMatch "(^|/)remount$" $repository -}}
{{- fail (printf "image.repository %q is the control-plane image, which is FROM scratch and has no userland; a process-backend workspace in it cannot run `sh`. Use the node image built by deploy/compose/node.Dockerfile." $repository) -}}
{{- end -}}
{{- printf "%s@%s" $repository $digest -}}
{{- end -}}

{{/*
Capability label arguments, sorted so the rendered manifest is stable and a
golden diff shows real change rather than map ordering.
*/}}
{{- define "remount-node.labelArgs" -}}
{{- range $k, $v := .Values.capabilityLabels }}
- --label={{ $k }}={{ $v }}
{{- end }}
{{- end -}}

{{/*
Refuse configurations in which Kubernetes would be asserting a fact the Remount
control plane owns, or in which provider metadata would carry a reusable
credential. Both are the two-owners failure Plan B phase B5 exists to reject,
and both are rejected here at render time rather than described in a comment.
*/}}
{{- define "remount-node.validateOwnership" -}}
{{- $runtime := list "workspace" "workspace_id" "session" "session_id" "claim" "lease" "generation" "checkpoint" "snapshot" -}}
{{- range $k, $v := .Values.capabilityLabels }}
{{- if has $k $runtime }}
{{- fail (printf "capabilityLabels.%s: labels describe what a node can do, not what is running on it. Workspace, session, claim, lease, generation and checkpoint are the Remount control plane's facts." $k) }}
{{- end }}
{{- if regexMatch "(?i)(secret|token|password|passwd|credential|private[_-]?key|api[_-]?key)$" $k }}
{{- fail (printf "capabilityLabels.%s: node labels are cluster-readable metadata and must not name a credential. Secrets reach Remount through the broker." $k) }}
{{- end }}
{{- if regexMatch "(sk-[A-Za-z0-9_-]{16,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|ghp_[A-Za-z0-9]{20,})" $v }}
{{- fail (printf "capabilityLabels.%s carries a value shaped like a reusable credential. The workspace is trusted with nothing; the broker substitutes secrets at the network edge." $k) }}
{{- end }}
{{- end }}
{{- range $arg := .Values.node.extraArgs }}
{{- if regexMatch "^(ws|session|exec|snapshot|checkpoint)( |$)" $arg }}
{{- fail (printf "node.extraArgs %q is a runtime workspace operation. This chart schedules nodes; it does not drive workspace lifecycle." $arg) }}
{{- end }}
{{- end }}
{{- end -}}
