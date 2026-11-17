{{- /* Modified copy of elastic/crd-ref-docs v0.3.0 templates/markdown/gv_details.tpl (Apache-2.0, see NOTICE): blank lines trimmed. */ -}}
{{- define "gvDetails" -}}
{{- $gv := . -}}

## {{ $gv.GroupVersionString }}
{{ with trim $gv.Doc }}
{{ . }}
{{ end }}
{{- if $gv.Kinds }}
### Resource Types
{{- range $gv.SortedKinds }}
- {{ $gv.TypeForKind . | markdownRenderTypeLink }}
{{- end }}
{{ end }}
{{- range $gv.SortedTypes }}
{{- template "type" . }}
{{- end }}
{{- end -}}
