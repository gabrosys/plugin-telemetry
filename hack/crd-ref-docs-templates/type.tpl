{{- /* Copyright 2020 Elasticsearch B.V. Copied from elastic/crd-ref-docs v0.3.0 templates/markdown (Apache-2.0). Changed: an alias renders its underlying type with markdownRenderType, so a map is not a dead link; no Default column; blank lines trimmed. */ -}}
{{- define "type" -}}
{{- $type := . -}}
{{- if markdownShouldRenderType $type }}
#### {{ $type.Name }}
{{ if $type.IsAlias }}
_Underlying type:_ _{{ markdownRenderType $type.UnderlyingType }}_
{{ end }}
{{ $type.Doc }}
{{ if $type.Validation }}
_Validation:_
{{- range $type.Validation }}
- {{ . }}
{{- end }}
{{ end }}
{{- if $type.References }}
_Appears in:_
{{- range $type.SortedReferences }}
- {{ markdownRenderTypeLink . }}
{{- end }}
{{ end }}
{{- if $type.Members }}
| Field | Description | Validation |
| --- | --- | --- |
{{ if $type.GVK -}}
| `apiVersion` _string_ | `{{ $type.GVK.Group }}/{{ $type.GVK.Version }}` | |
| `kind` _string_ | `{{ $type.GVK.Kind }}` | |
{{ end -}}
{{ range $type.Members -}}
| `{{ .Name  }}` _{{ markdownRenderType .Type }}_ | {{ template "type_members" . }} | {{ range .Validation -}} {{ markdownRenderFieldDoc . }} <br />{{ end }} |
{{ end -}}
{{ end }}
{{- if $type.EnumValues }}
| Field | Description |
| --- | --- |
{{ range $type.EnumValues -}}
| `{{ .Name }}` | {{ markdownRenderFieldDoc .Doc }} |
{{ end -}}
{{ end }}
{{- end -}}
{{- end -}}
