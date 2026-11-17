{{- /* Modified copy of elastic/crd-ref-docs v0.3.0 templates/markdown/gv_list.tpl (Apache-2.0, see NOTICE): blank lines trimmed. */ -}}
{{- define "gvList" -}}
{{- $groupVersions := . -}}

# API Reference

## Packages
{{- range $groupVersions }}
- {{ markdownRenderGVLink . }}
{{- end }}
{{ range $groupVersions }}
{{ template "gvDetails" . }}{{ end }}
{{- end -}}
