package factory

import "testing"

func TestScanners_Name(t *testing.T) {
	if (GoogleMapsScanner{}).Name() != "google_maps" {
		t.Fatalf("unexpected google maps scanner name")
	}
	if (InstagramScanner{}).Name() != "instagram" {
		t.Fatalf("unexpected instagram scanner name")
	}
	if (ReviewScanner{}).Name() != "reviews" {
		t.Fatalf("unexpected review scanner name")
	}
}
