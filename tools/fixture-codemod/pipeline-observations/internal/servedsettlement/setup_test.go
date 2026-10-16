package servedsettlement

import "testing"

func TestSetupCurrentBodyCacheUsesCompleteBytesAndDeclarationIdentity(t *testing.T) {
	valid := []byte("package proof\nfunc first() { original() }; func second() { other() }")
	first, err := setupCurrentBody(valid, "first")
	if err != nil {
		t.Fatal(err)
	}
	if second, err := setupCurrentBody(valid, "second"); err != nil || first == second {
		t.Fatal("cache conflated separate declarations:", err)
	}
	for _, raw := range []string{
		"package proof\nfunc first() { changed() }; func second() { other() }",
		"package proof\nfunc first() { original() }; func first() { original() }",
		"package proof\nfunc second() { original() }",
		"package proof\nfunc first() {",
	} {
		if got, err := setupCurrentBody([]byte(raw), "first"); err == nil && got == first {
			t.Fatal("warm cache concealed a changed, duplicate, missing or malformed declaration")
		}
	}
	if got, err := setupCurrentBody(valid, "first"); err != nil || got != first {
		t.Fatal("unchanged exact bytes did not retain their parse:", err)
	}
}
