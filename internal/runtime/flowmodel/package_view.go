package flowmodel

type PackageView[P, M, N, E, A, T any] struct {
	Paths    P
	Manifest M
	Nodes    map[string]N
	Events   map[string]E
	Agents   map[string]A
	Tools    map[string]T
	Policy   PolicyDocument
}
