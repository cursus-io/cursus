{{- define "cursus-cluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cursus-cluster.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "cursus-cluster.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "cursus-cluster.labels" -}}
app.kubernetes.io/name: {{ include "cursus-cluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cursus-cluster.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cursus-cluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "cursus-cluster.headlessService" -}}
{{ include "cursus-cluster.fullname" . }}-headless
{{- end }}

{{- define "cursus-cluster.memberHost" -}}
{{- $ordinal := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- printf "%s-%d.%s.%s.svc.cluster.local" (include "cursus-cluster.fullname" $ctx) $ordinal (include "cursus-cluster.headlessService" $ctx) $ctx.Release.Namespace -}}
{{- end }}

{{- define "cursus-cluster.members" -}}
{{- $brokerPort := int .Values.service.brokerPort -}}
{{- $raftPort := int .Values.service.raftPort -}}
{{- range $ordinal := until 3 -}}
{{- if $ordinal }},{{ end -}}
{{- $host := include "cursus-cluster.memberHost" (list $ordinal $) -}}
{{ printf "%s-%d@%s:%d" $host $brokerPort $host $raftPort -}}
{{- end -}}
{{- end }}
