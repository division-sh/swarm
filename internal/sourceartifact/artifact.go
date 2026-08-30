package sourceartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/yamlsource"
)

const (
	HashPrefix       = "bundle-v2:sha256:"
	logicalPrelude   = "swarm-bundle-v2\x00"
	MaxMembers       = 4096
	MaxMemberBytes   = 16 << 20
	MaxArtifactBytes = 48 << 20
	MaxLabelBytes    = 255
	MaxPathSegments  = 32
	MaxSegmentBytes  = 100
)

type Disposition uint8

const (
	DispositionDeclaration Disposition = iota + 1
	DispositionManifest
	DispositionResource
	DispositionDocument
)

func dispositionFromCode(code byte) (Disposition, error) {
	disposition := Disposition(code)
	switch disposition {
	case DispositionDeclaration, DispositionManifest, DispositionResource, DispositionDocument:
		return disposition, nil
	default:
		return 0, fmt.Errorf("source artifact disposition code %d is invalid", code)
	}
}

func (d Disposition) String() string {
	switch d {
	case DispositionDeclaration:
		return "declaration"
	case DispositionManifest:
		return "manifest"
	case DispositionResource:
		return "resource"
	case DispositionDocument:
		return "document"
	default:
		return "unknown"
	}
}

type Entry struct {
	label       string
	disposition Disposition
	body        []byte
}

func (e Entry) Label() string            { return e.label }
func (e Entry) Disposition() Disposition { return e.disposition }
func (e Entry) Bytes() []byte            { return append([]byte(nil), e.body...) }
func (e Entry) Size() int                { return len(e.body) }

type FlowNode struct {
	flowPath     string
	declarations map[string]string
	resources    map[string][]string
	documents    []string
	manifest     string
	children     []*FlowNode
}

func (n *FlowNode) Path() string {
	if n == nil {
		return ""
	}
	return n.flowPath
}

func (n *FlowNode) Declaration(name string) (string, bool) {
	if n == nil {
		return "", false
	}
	label, ok := n.declarations[name]
	return label, ok
}

func (n *FlowNode) Manifest() (string, bool) {
	if n == nil || n.manifest == "" {
		return "", false
	}
	return n.manifest, true
}

func (n *FlowNode) Resources(branch string) []string {
	if n == nil {
		return nil
	}
	return append([]string(nil), n.resources[branch]...)
}

func (n *FlowNode) Documents() []string {
	if n == nil {
		return nil
	}
	return append([]string(nil), n.documents...)
}

func (n *FlowNode) Children() []*FlowNode {
	if n == nil {
		return nil
	}
	out := make([]*FlowNode, 0, len(n.children))
	for _, child := range n.children {
		out = append(out, cloneFlowNode(child))
	}
	return out
}

type AdmittedSourceArtifact struct {
	entries    []Entry
	byLabel    map[string]int
	yaml       map[string]yamlsource.Snapshot
	root       *FlowNode
	logical    []byte
	bundleHash string
}

func newArtifact(entries []Entry) (*AdmittedSourceArtifact, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("source artifact contains no admitted members")
	}
	if len(entries) > MaxMembers {
		return nil, fmt.Errorf("source artifact has %d members, maximum is %d", len(entries), MaxMembers)
	}

	owned := make([]Entry, len(entries))
	byLabel := make(map[string]int, len(entries))
	yamlSnapshots := make(map[string]yamlsource.Snapshot)
	folded := make(map[string]string, len(entries))
	total := 0
	for index, candidate := range entries {
		label := candidate.label
		if err := ValidateLabel(label); err != nil {
			return nil, err
		}
		if _, exists := byLabel[label]; exists {
			return nil, fmt.Errorf("duplicate source artifact label %q", label)
		}
		fold := asciiFold(label)
		if previous, exists := folded[fold]; exists {
			return nil, fmt.Errorf("case-colliding source artifact labels %q and %q", previous, label)
		}
		if len(candidate.body) > MaxMemberBytes {
			return nil, fmt.Errorf("source artifact member %q is %d bytes, maximum is %d", label, len(candidate.body), MaxMemberBytes)
		}
		total += len(candidate.body)
		if total > MaxArtifactBytes {
			return nil, fmt.Errorf("source artifact is %d bytes, maximum is %d", total, MaxArtifactBytes)
		}
		disposition, err := classifyLabel(label)
		if err != nil {
			return nil, err
		}
		if candidate.disposition != 0 && candidate.disposition != disposition {
			return nil, fmt.Errorf("source artifact member %q disposition %s does not match canonical %s", label, candidate.disposition, disposition)
		}
		body := append([]byte(nil), candidate.body...)
		owned[index] = Entry{label: label, disposition: disposition, body: body}
		byLabel[label] = index
		folded[fold] = label
		if isAdmittedYAML(label, disposition) {
			snapshot, err := yamlsource.Load(body)
			if err != nil {
				return nil, fmt.Errorf("parse admitted YAML %s: %w", label, err)
			}
			yamlSnapshots[label] = snapshot
		}
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].label < owned[j].label })
	byLabel = make(map[string]int, len(owned))
	for index := range owned {
		byLabel[owned[index].label] = index
	}
	root, err := buildFlowTree(owned)
	if err != nil {
		return nil, err
	}
	logical, err := encodeLogical(owned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(logical)
	return &AdmittedSourceArtifact{
		entries: owned, byLabel: byLabel, yaml: yamlSnapshots, root: root,
		logical: logical, bundleHash: HashPrefix + hex.EncodeToString(digest[:]),
	}, nil
}

func (a *AdmittedSourceArtifact) BundleHash() string {
	if a == nil {
		return ""
	}
	return a.bundleHash
}

func (a *AdmittedSourceArtifact) LogicalBlob() []byte {
	if a == nil {
		return nil
	}
	return append([]byte(nil), a.logical...)
}

func (a *AdmittedSourceArtifact) Entries() []Entry {
	if a == nil {
		return nil
	}
	out := make([]Entry, len(a.entries))
	for index := range a.entries {
		out[index] = Entry{label: a.entries[index].label, disposition: a.entries[index].disposition, body: append([]byte(nil), a.entries[index].body...)}
	}
	return out
}

func (a *AdmittedSourceArtifact) Entry(label string) (Entry, bool) {
	if a == nil {
		return Entry{}, false
	}
	index, ok := a.byLabel[label]
	if !ok {
		return Entry{}, false
	}
	entry := a.entries[index]
	entry.body = append([]byte(nil), entry.body...)
	return entry, true
}

func (a *AdmittedSourceArtifact) YAML(label string) (yamlsource.Document, bool) {
	if a == nil {
		return yamlsource.MissingDocument(label), false
	}
	snapshot, ok := a.yaml[label]
	if !ok {
		return yamlsource.MissingDocument(label), false
	}
	return snapshot.Document(label), true
}

func (a *AdmittedSourceArtifact) YAMLRoot(label string) (yamlsource.Node, bool) {
	if a == nil {
		return yamlsource.Node{}, false
	}
	snapshot, ok := a.yaml[label]
	if !ok {
		return yamlsource.Node{}, false
	}
	return snapshot.Root(), true
}

func (a *AdmittedSourceArtifact) DecodeYAML(label string, target any) error {
	if a == nil {
		return fmt.Errorf("source artifact is required")
	}
	snapshot, ok := a.yaml[label]
	if !ok {
		return &fs.PathError{Op: "decode", Path: label, Err: fs.ErrNotExist}
	}
	return snapshot.Decode(target)
}

func (a *AdmittedSourceArtifact) Root() *FlowNode {
	if a == nil {
		return nil
	}
	return cloneFlowNode(a.root)
}

func (a *AdmittedSourceArtifact) FS() fs.FS { return artifactFS{artifact: a} }

func DecodeLogical(blob []byte) (*AdmittedSourceArtifact, error) {
	return decodeLogicalWithinLimit(blob, MaxArtifactBytes)
}

func decodeLogicalWithinLimit(blob []byte, maxArtifactBytes uint64) (*AdmittedSourceArtifact, error) {
	reader := bytes.NewReader(blob)
	prefix := make([]byte, len(logicalPrelude))
	if _, err := io.ReadFull(reader, prefix); err != nil || string(prefix) != logicalPrelude {
		return nil, fmt.Errorf("source artifact blob has invalid bundle-v2 prelude")
	}
	count, err := readU64(reader)
	if err != nil {
		return nil, fmt.Errorf("decode source artifact member count: %w", err)
	}
	if count == 0 || count > MaxMembers {
		return nil, fmt.Errorf("source artifact member count %d is outside 1..%d", count, MaxMembers)
	}
	entries := make([]Entry, 0, int(count))
	var decodedBytes uint64
	for index := uint64(0); index < count; index++ {
		dispositionCode, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("decode source artifact member %d disposition: %w", index, err)
		}
		disposition, err := dispositionFromCode(dispositionCode)
		if err != nil {
			return nil, fmt.Errorf("decode source artifact member %d disposition: %w", index, err)
		}
		labelLength, err := readU64(reader)
		if err != nil || labelLength == 0 || labelLength > MaxLabelBytes {
			return nil, fmt.Errorf("decode source artifact member %d label length", index)
		}
		label := make([]byte, int(labelLength))
		if _, err := io.ReadFull(reader, label); err != nil {
			return nil, fmt.Errorf("decode source artifact member %d label: %w", index, err)
		}
		contentLength, err := readU64(reader)
		if err != nil || contentLength > MaxMemberBytes || contentLength > uint64(reader.Len()) {
			return nil, fmt.Errorf("decode source artifact member %q content length", string(label))
		}
		total := decodedBytes + contentLength
		if total > maxArtifactBytes {
			return nil, fmt.Errorf("source artifact is %d bytes, maximum is %d", total, maxArtifactBytes)
		}
		decodedBytes = total
		body := make([]byte, int(contentLength))
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, fmt.Errorf("decode source artifact member %q: %w", string(label), err)
		}
		entries = append(entries, Entry{label: string(label), disposition: disposition, body: body})
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("source artifact blob has %d trailing bytes", reader.Len())
	}
	artifact, err := newArtifact(entries)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(artifact.logical, blob) {
		return nil, fmt.Errorf("source artifact blob is not in canonical raw-ASCII label order")
	}
	return artifact, nil
}

func ValidateHash(value string) error {
	if len(value) != len(HashPrefix)+sha256.Size*2 || !strings.HasPrefix(value, HashPrefix) {
		return fmt.Errorf("bundle_hash must be %s<64 lowercase hex>", HashPrefix)
	}
	for _, char := range value[len(HashPrefix):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return fmt.Errorf("bundle_hash must be %s<64 lowercase hex>", HashPrefix)
		}
	}
	return nil
}

func ValidateLabel(label string) error {
	if label == "" || len(label) > MaxLabelBytes || !utf8.ValidString(label) {
		return fmt.Errorf("source artifact label %q must be 1..%d ASCII bytes", label, MaxLabelBytes)
	}
	for _, char := range []byte(label) {
		if char > 0x7f {
			return fmt.Errorf("source artifact label %q must be ASCII", label)
		}
	}
	if strings.HasPrefix(label, "/") || strings.HasSuffix(label, "/") || strings.Contains(label, "//") || strings.Contains(label, "\\") {
		return fmt.Errorf("source artifact label %q is not selected-root-relative", label)
	}
	segments := strings.Split(label, "/")
	if len(segments) > MaxPathSegments {
		return fmt.Errorf("source artifact label %q has %d segments, maximum is %d", label, len(segments), MaxPathSegments)
	}
	for _, segment := range segments {
		if err := validatePortableSegment(segment); err != nil {
			return fmt.Errorf("source artifact label %q: %w", label, err)
		}
	}
	return nil
}

func encodeLogical(entries []Entry) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString(logicalPrelude)
	writeU64(&out, uint64(len(entries)))
	for _, entry := range entries {
		if _, err := dispositionFromCode(byte(entry.disposition)); err != nil {
			return nil, fmt.Errorf("encode source artifact member %q disposition: %w", entry.label, err)
		}
		out.WriteByte(byte(entry.disposition))
		writeU64(&out, uint64(len(entry.label)))
		out.WriteString(entry.label)
		writeU64(&out, uint64(len(entry.body)))
		out.Write(entry.body)
	}
	return out.Bytes(), nil
}

func writeU64(writer io.Writer, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	_, _ = writer.Write(raw[:])
}

func readU64(reader io.Reader) (uint64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func cloneFlowNode(source *FlowNode) *FlowNode {
	if source == nil {
		return nil
	}
	out := &FlowNode{
		flowPath: source.flowPath, manifest: source.manifest,
		declarations: make(map[string]string, len(source.declarations)),
		resources:    make(map[string][]string, len(source.resources)),
		documents:    append([]string(nil), source.documents...),
	}
	for key, value := range source.declarations {
		out.declarations[key] = value
	}
	for key, value := range source.resources {
		out.resources[key] = append([]string(nil), value...)
	}
	for _, child := range source.children {
		out.children = append(out.children, cloneFlowNode(child))
	}
	return out
}

type artifactFS struct{ artifact *AdmittedSourceArtifact }

func (f artifactFS) Open(name string) (fs.File, error) {
	if f.artifact == nil {
		return nil, fs.ErrNotExist
	}
	clean := path.Clean(name)
	if clean == "." {
		return newArtifactDir(f.artifact, "."), nil
	}
	if clean != name || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if entry, ok := f.artifact.Entry(clean); ok {
		return &artifactFile{reader: *bytes.NewReader(entry.body), info: artifactInfo{name: path.Base(clean), size: int64(len(entry.body))}}, nil
	}
	if artifactHasDir(f.artifact, clean) {
		return newArtifactDir(f.artifact, clean), nil
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

type artifactInfo struct {
	name string
	size int64
	dir  bool
}

func (i artifactInfo) Name() string { return i.name }
func (i artifactInfo) Size() int64  { return i.size }
func (i artifactInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (i artifactInfo) ModTime() time.Time { return time.Time{} }
func (i artifactInfo) IsDir() bool        { return i.dir }
func (i artifactInfo) Sys() any           { return nil }

type artifactFile struct {
	reader bytes.Reader
	info   artifactInfo
}

func (f *artifactFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *artifactFile) Read(p []byte) (int, error) { return f.reader.Read(p) }
func (f *artifactFile) Close() error               { return nil }

type artifactDir struct {
	info    artifactInfo
	entries []fs.DirEntry
	offset  int
}

func newArtifactDir(artifact *AdmittedSourceArtifact, name string) *artifactDir {
	children := artifactDirChildren(artifact, name)
	return &artifactDir{info: artifactInfo{name: path.Base(name), dir: true}, entries: children}
}
func (d *artifactDir) Stat() (fs.FileInfo, error) { return d.info, nil }
func (d *artifactDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.name, Err: fs.ErrInvalid}
}
func (d *artifactDir) Close() error { return nil }
func (d *artifactDir) ReadDir(count int) ([]fs.DirEntry, error) {
	if d.offset >= len(d.entries) && count > 0 {
		return nil, io.EOF
	}
	end := len(d.entries)
	if count > 0 && d.offset+count < end {
		end = d.offset + count
	}
	out := append([]fs.DirEntry(nil), d.entries[d.offset:end]...)
	d.offset = end
	return out, nil
}

type artifactDirEntry struct{ info artifactInfo }

func (e artifactDirEntry) Name() string               { return e.info.name }
func (e artifactDirEntry) IsDir() bool                { return e.info.dir }
func (e artifactDirEntry) Type() fs.FileMode          { return e.info.Mode().Type() }
func (e artifactDirEntry) Info() (fs.FileInfo, error) { return e.info, nil }

func artifactHasDir(artifact *AdmittedSourceArtifact, dir string) bool {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for _, entry := range artifact.entries {
		if strings.HasPrefix(entry.label, prefix) {
			return true
		}
	}
	return false
}

func artifactDirChildren(artifact *AdmittedSourceArtifact, dir string) []fs.DirEntry {
	prefix := ""
	if dir != "." {
		prefix = strings.TrimSuffix(dir, "/") + "/"
	}
	children := map[string]artifactInfo{}
	for _, entry := range artifact.entries {
		if !strings.HasPrefix(entry.label, prefix) {
			continue
		}
		rest := strings.TrimPrefix(entry.label, prefix)
		name, tail, _ := strings.Cut(rest, "/")
		info := artifactInfo{name: name, size: int64(len(entry.body)), dir: tail != ""}
		if previous, ok := children[name]; !ok || info.dir {
			children[name] = info
		} else {
			children[name] = previous
		}
	}
	names := make([]string, 0, len(children))
	for name := range children {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]fs.DirEntry, 0, len(names))
	for _, name := range names {
		out = append(out, artifactDirEntry{info: children[name]})
	}
	return out
}
