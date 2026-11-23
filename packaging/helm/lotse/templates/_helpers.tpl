{{/* The release's name, cut to fit the names Kubernetes derives from it. */}}
{{- define "lotse.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "lotse.headless" -}}
{{- printf "%s-nodes" (include "lotse.fullname" .) -}}
{{- end -}}

{{- define "lotse.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "lotse.labels" -}}
{{ include "lotse.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* The DNS name of the pod with index $i. */}}
{{- define "lotse.podHost" -}}
{{- $root := index . 0 -}}
{{- $i := index . 1 -}}
{{- printf "%s-%d.%s.%s.svc.cluster.local" (include "lotse.fullname" $root) $i (include "lotse.headless" $root) $root.Release.Namespace -}}
{{- end -}}

{{/* The console's resources (ADR 0137). Its pods carry another name label,
     so the nodes' Services never select them. */}}
{{- define "lotse.console.fullname" -}}
{{- printf "%s-console" (include "lotse.fullname" .) -}}
{{- end -}}

{{- define "lotse.console.selectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-console" .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "lotse.console.labels" -}}
{{ include "lotse.console.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.console.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
