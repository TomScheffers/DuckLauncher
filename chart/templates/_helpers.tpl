{{- define "ducklauncher.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ducklauncher.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "ducklauncher.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ducklauncher.labels" -}}
helm.sh/chart: {{ include "ducklauncher.chart" . }}
{{ include "ducklauncher.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "ducklauncher.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ducklauncher.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "ducklauncher.coordinator.selectorLabels" -}}
{{ include "ducklauncher.selectorLabels" . }}
app.kubernetes.io/component: coordinator
{{- end }}

{{- define "ducklauncher.worker.selectorLabels" -}}
{{ include "ducklauncher.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{- define "ducklauncher.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ducklauncher.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "ducklauncher.coordinator.serviceName" -}}
{{- printf "%s-coordinator" (include "ducklauncher.fullname" .) }}
{{- end }}

{{- define "ducklauncher.worker.serviceName" -}}
{{- printf "%s-worker" (include "ducklauncher.fullname" .) }}
{{- end }}

{{- define "ducklauncher.coordinator.url" -}}
{{- printf "http://%s:%v" (include "ducklauncher.coordinator.serviceName" .) .Values.coordinator.service.port }}
{{- end }}

{{- define "ducklauncher.database.secretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "ducklauncher.fullname" . }}
{{- end }}
{{- end }}

{{- define "ducklauncher.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- end }}
