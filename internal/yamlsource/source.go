package yamlsource

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

const (
	DefaultMaxEntries     = 256
	DefaultMaxSourceBytes = 64 << 20
)

type Digest [sha256.Size]byte

type Limits struct {
	MaxEntries     int
	MaxSourceBytes int64
}

type Store struct {
	mu          sync.Mutex
	limits      Limits
	entries     map[Digest]*entry
	inflight    map[Digest]*entry
	lru         *list.List
	sourceBytes int64
	parseCount  uint64
	hits        uint64
	coalesced   uint64
	parse       func([]byte) (yaml.Node, error)
}

type entry struct {
	digest Digest
	raw    []byte
	root   yaml.Node
	err    error
	ready  chan struct{}
	lru    *list.Element
}

type Snapshot struct {
	entry *entry
}

type ParseError struct {
	cause error
}

func (e *ParseError) Error() string { return e.cause.Error() }
func (e *ParseError) Unwrap() error { return e.cause }

func ParseCause(err error) (error, bool) {
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		return nil, false
	}
	return parseErr.cause, true
}

type Stats struct {
	Entries     int
	Inflight    int
	SourceBytes int64
	ParseCount  uint64
	Hits        uint64
	Coalesced   uint64
}

var defaultStore = NewStore(Limits{
	MaxEntries:     DefaultMaxEntries,
	MaxSourceBytes: DefaultMaxSourceBytes,
})

func NewStore(limits Limits) *Store {
	if limits.MaxEntries < 0 {
		limits.MaxEntries = 0
	}
	if limits.MaxSourceBytes < 0 {
		limits.MaxSourceBytes = 0
	}
	return &Store{
		limits:   limits,
		entries:  make(map[Digest]*entry),
		inflight: make(map[Digest]*entry),
		lru:      list.New(),
		parse:    parse,
	}
}

func LoadFile(path string) (Snapshot, error) {
	return defaultStore.LoadFile(path)
}

func Load(raw []byte) (Snapshot, error) {
	return defaultStore.Load(raw)
}

func DefaultStats() Stats {
	return defaultStore.Stats()
}

func (s *Store) LoadFile(path string) (Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	return s.Load(raw)
}

func (s *Store) Load(raw []byte) (Snapshot, error) {
	owned := append([]byte(nil), raw...)
	digest := sha256.Sum256(owned)

	s.mu.Lock()
	if cached := s.entries[digest]; cached != nil {
		s.hits++
		s.lru.MoveToFront(cached.lru)
		s.mu.Unlock()
		return snapshotResult(cached)
	}
	if pending := s.inflight[digest]; pending != nil {
		ready := pending.ready
		s.coalesced++
		s.mu.Unlock()
		<-ready
		return snapshotResult(pending)
	}

	pending := &entry{digest: digest, raw: owned, ready: make(chan struct{})}
	s.inflight[digest] = pending
	s.mu.Unlock()

	pending.root, pending.err = s.parse(owned)
	if pending.err != nil {
		pending.err = &ParseError{cause: pending.err}
	}

	s.mu.Lock()
	s.parseCount++
	delete(s.inflight, digest)
	if s.retainable(pending) {
		pending.lru = s.lru.PushFront(pending)
		s.entries[digest] = pending
		s.sourceBytes += int64(len(pending.raw))
		s.evict()
	}
	close(pending.ready)
	s.mu.Unlock()

	return snapshotResult(pending)
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Entries:     len(s.entries),
		Inflight:    len(s.inflight),
		SourceBytes: s.sourceBytes,
		ParseCount:  s.parseCount,
		Hits:        s.hits,
		Coalesced:   s.coalesced,
	}
}

func (s *Store) retainable(candidate *entry) bool {
	return s.limits.MaxEntries > 0 &&
		s.limits.MaxSourceBytes > 0 &&
		int64(len(candidate.raw)) <= s.limits.MaxSourceBytes
}

func (s *Store) evict() {
	for len(s.entries) > s.limits.MaxEntries || s.sourceBytes > s.limits.MaxSourceBytes {
		oldest := s.lru.Back()
		if oldest == nil {
			return
		}
		victim := oldest.Value.(*entry)
		delete(s.entries, victim.digest)
		s.sourceBytes -= int64(len(victim.raw))
		s.lru.Remove(oldest)
		victim.lru = nil
	}
}

func snapshotResult(candidate *entry) (Snapshot, error) {
	if candidate.err != nil {
		return Snapshot{}, candidate.err
	}
	return Snapshot{entry: candidate}, nil
}

func parse(raw []byte) (yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		if err == io.EOF {
			return yaml.Node{}, fmt.Errorf("YAML input has no document")
		}
		return yaml.Node{}, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return yaml.Node{}, fmt.Errorf("YAML input has multiple documents")
		}
		return yaml.Node{}, err
	}
	return root, nil
}

func (s Snapshot) Digest() Digest {
	if s.entry == nil {
		return Digest{}
	}
	return s.entry.digest
}

func (s Snapshot) Bytes() []byte {
	if s.entry == nil {
		return nil
	}
	return append([]byte(nil), s.entry.raw...)
}

func (s Snapshot) Decode(target any) error {
	if s.entry == nil {
		return fmt.Errorf("empty YAML snapshot")
	}
	root := cloneNode(&s.entry.root)
	return root.Decode(target)
}

func (s Snapshot) Root() Node {
	if s.entry == nil {
		return Node{}
	}
	return Node{node: &s.entry.root}
}

func (s Snapshot) NodeCopy() yaml.Node {
	if s.entry == nil {
		return yaml.Node{}
	}
	return *cloneNode(&s.entry.root)
}

func cloneNode(source *yaml.Node) *yaml.Node {
	if source == nil {
		return nil
	}
	nodeCount, edgeCount := nodeTreeSize(source)
	cloner := nodeGraphCloner{
		nodes: make([]yaml.Node, nodeCount),
		edges: make([]*yaml.Node, edgeCount),
		seen:  make(map[*yaml.Node]*yaml.Node, nodeCount),
	}
	return cloner.clone(source)
}

func nodeTreeSize(source *yaml.Node) (nodes, edges int) {
	if source == nil {
		return 0, 0
	}
	nodes = 1
	edges = len(source.Content)
	for _, child := range source.Content {
		childNodes, childEdges := nodeTreeSize(child)
		nodes += childNodes
		edges += childEdges
	}
	// yaml.v3 parser aliases point at anchored nodes already present in the
	// document Content tree, so they do not require additional arena slots.
	return nodes, edges
}

type nodeGraphCloner struct {
	nodes    []yaml.Node
	edges    []*yaml.Node
	seen     map[*yaml.Node]*yaml.Node
	nextNode int
	nextEdge int
}

func (c *nodeGraphCloner) clone(source *yaml.Node) *yaml.Node {
	if source == nil {
		return nil
	}
	if existing := c.seen[source]; existing != nil {
		return existing
	}

	clone := &c.nodes[c.nextNode]
	c.nextNode++
	*clone = *source
	clone.Content = nil
	clone.Alias = nil
	c.seen[source] = clone

	if len(source.Content) > 0 {
		start := c.nextEdge
		c.nextEdge += len(source.Content)
		clone.Content = c.edges[start:c.nextEdge]
		for i, child := range source.Content {
			clone.Content[i] = c.clone(child)
		}
	}
	clone.Alias = c.clone(source.Alias)
	return clone
}

func (s Snapshot) LookupMapPath(parts ...string) (Node, bool) {
	node := s.Root()
	if node.Kind() == DocumentNode {
		if node.Len() != 1 {
			return Node{}, false
		}
		node, _ = node.Child(0)
	}
	for _, part := range parts {
		if node.Kind() != MappingNode {
			return Node{}, false
		}
		found := false
		for i := 0; i+1 < node.Len(); i += 2 {
			key, _ := node.Child(i)
			if key.Value() == part {
				node, _ = node.Child(i + 1)
				found = true
				break
			}
		}
		if !found {
			return Node{}, false
		}
	}
	return node, true
}

type Kind uint32

const (
	DocumentNode Kind = Kind(yaml.DocumentNode)
	SequenceNode Kind = Kind(yaml.SequenceNode)
	MappingNode  Kind = Kind(yaml.MappingNode)
	ScalarNode   Kind = Kind(yaml.ScalarNode)
	AliasNode    Kind = Kind(yaml.AliasNode)
)

type Style uint32

const (
	TaggedStyle       Style = Style(yaml.TaggedStyle)
	DoubleQuotedStyle Style = Style(yaml.DoubleQuotedStyle)
	SingleQuotedStyle Style = Style(yaml.SingleQuotedStyle)
)

type Node struct {
	node *yaml.Node
}

func (n Node) Valid() bool { return n.node != nil }

func (n Node) Kind() Kind {
	if n.node == nil {
		return 0
	}
	return Kind(n.node.Kind)
}

func (n Node) Tag() string {
	if n.node == nil {
		return ""
	}
	return n.node.Tag
}

func (n Node) Value() string {
	if n.node == nil {
		return ""
	}
	return n.node.Value
}

func (n Node) Line() int {
	if n.node == nil {
		return 0
	}
	return n.node.Line
}

func (n Node) Style() Style {
	if n.node == nil {
		return 0
	}
	return Style(n.node.Style)
}

func (n Node) Len() int {
	if n.node == nil {
		return 0
	}
	return len(n.node.Content)
}

func (n Node) Child(index int) (Node, bool) {
	if n.node == nil || index < 0 || index >= len(n.node.Content) {
		return Node{}, false
	}
	return Node{node: n.node.Content[index]}, true
}

func (n Node) Alias() (Node, bool) {
	if n.node == nil || n.node.Alias == nil {
		return Node{}, false
	}
	return Node{node: n.node.Alias}, true
}
