package connectorapi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/providerconnectors"
)

type GitHub struct {
	Server      *httptest.Server
	PrivateKey  string
	Calls       atomic.Int64
	TokenCalls  atomic.Int64
	SideEffects chan Call
	Fixture     providerconnectors.CatalogConformanceFixture
}

// NewGitHub uses the catalog's reviewed issues/create request, not a second schema.
func NewGitHub(t testing.TB, fixturePath string) *GitHub {
	t.Helper()
	fake := &GitHub{SideEffects: make(chan Call, 8)}
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	fake.Fixture, err = providerconnectors.ParseCatalogConformanceFixture(raw)
	if err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fake.PrivateKey = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	mux := http.NewServeMux()
	mux.HandleFunc("/app/installations/1001/access_tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Accept") != "application/vnd.github+json" {
			http.Error(w, "exact installation-token request required", http.StatusBadRequest)
			return
		}
		if !validGitHubJWT(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), &key.PublicKey) {
			http.Error(w, "valid app JWT required", http.StatusUnauthorized)
			return
		}
		fake.TokenCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"token": fake.Fixture.ManagedCredentials["github_app"], "expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)})
	})
	mux.HandleFunc("/repos/octocat/hello-world/issues", func(w http.ResponseWriter, r *http.Request) {
		fake.Calls.Add(1)
		for name, expected := range fake.Fixture.Expected.Headers {
			if r.Header.Get(name) != expected {
				http.Error(w, "catalog request header mismatch: "+name, http.StatusUnauthorized)
				return
			}
		}
		raw, err := io.ReadAll(r.Body)
		var body map[string]any
		if err != nil || json.Unmarshal(raw, &body) != nil || r.Method != fake.Fixture.Expected.Method || !reflect.DeepEqual(body, fake.Fixture.Expected.Body) {
			http.Error(w, "catalog request mismatch", http.StatusBadRequest)
			return
		}
		fake.SideEffects <- Call{Method: r.Method, Path: r.URL.Path, Auth: r.Header.Get("Authorization"), Body: body, Raw: string(raw)}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 100, "number": 7, "title": body["title"]})
	})
	fake.Server = httptest.NewServer(mux)
	return fake
}

func validGitHubJWT(token string, key *rsa.PublicKey) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	header, hErr := base64.RawURLEncoding.DecodeString(parts[0])
	body, bErr := base64.RawURLEncoding.DecodeString(parts[1])
	signature, sErr := base64.RawURLEncoding.DecodeString(parts[2])
	var algorithm struct{ Alg, Typ string }
	var claims struct {
		Iss      string
		Iat, Exp int64
	}
	if hErr != nil || bErr != nil || sErr != nil || json.Unmarshal(header, &algorithm) != nil || json.Unmarshal(body, &claims) != nil {
		return false
	}
	now := time.Now().Unix()
	if algorithm.Alg != "RS256" || algorithm.Typ != "JWT" || claims.Iss != "github-app-client-id" || claims.Iat > now || claims.Exp <= now || claims.Exp-claims.Iat > 11*60 {
		return false
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], signature) == nil
}
