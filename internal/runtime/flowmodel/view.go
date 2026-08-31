package flowmodel

type View[P, S, N, E, A, T any] struct {
	Paths     P
	Schema    S
	Nodes     map[string]N
	Events    map[string]E
	Agents    map[string]A
	Tools     map[string]T
	Policy    PolicyDocument
	Path      string
	URI       string
	NodeURIs  map[string]string
	AgentURIs map[string]string
	EventURIs map[string]string
	Children  []View[P, S, N, E, A, T]
	Parent    *View[P, S, N, E, A, T]
}
