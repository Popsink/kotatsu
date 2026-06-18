{{/*
Expand the name of the chart.
*/}}
{{- define "kotatsu.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name + version label.
*/}}
{{- define "kotatsu.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Standard labels.
*/}}
{{- define "kotatsu.labels" -}}
helm.sh/chart: {{ include "kotatsu.chart" . }}
{{ include "kotatsu.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "kotatsu.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kotatsu.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use.
*/}}
{{- define "kotatsu.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "common.names.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Return the kotatsu image string.
*/}}
{{- define "kotatsu.image" -}}
{{- $registry := .Values.image.registry -}}
{{- if .Values.global.imageRegistry -}}
{{- $registry = .Values.global.imageRegistry -}}
{{- end -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion | toString -}}
{{- if .Values.image.digest -}}
{{- printf "%s/%s@%s" $registry .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s/%s:%s" $registry .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/*
Return image pull secrets.
*/}}
{{- define "kotatsu.imagePullSecrets" -}}
{{- $pullSecrets := concat (.Values.global.imagePullSecrets | default list) (.Values.image.pullSecrets | default list) -}}
{{- if $pullSecrets }}
imagePullSecrets:
{{- range $pullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end -}}
{{- end -}}

{{/*
Return the name of the storage credentials secret.
*/}}
{{- define "kotatsu.storageSecretName" -}}
{{- if .Values.storage.existingSecret -}}
{{- include "common.tplvalues.render" (dict "value" .Values.storage.existingSecret "context" $) -}}
{{- else -}}
{{- printf "%s-storage" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a storage credentials secret should be created.
*/}}
{{- define "kotatsu.createStorageSecret" -}}
{{- if and .Values.storage.accessKey .Values.storage.secretKey (not .Values.storage.existingSecret) -}}
true
{{- end -}}
{{- end -}}

{{/*
Return true if storage credentials are configured (static or existing secret).
*/}}
{{- define "kotatsu.hasStorageCredentials" -}}
{{- if or .Values.storage.existingSecret (and .Values.storage.accessKey .Values.storage.secretKey) -}}
true
{{- end -}}
{{- end -}}
