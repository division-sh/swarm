package serveapp

import "testing"

func TestS1ProcessWebhookPathKeepsRawProviderKey(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", " telegram_sales", "telegram_sales "} {
		alias, got, ok := parseProcessWebhookPath("/webhooks/shop/" + key)
		if !ok || alias != "shop" || got != key {
			t.Errorf("path changed raw key %q to %q alias=%q present=%v", key, got, alias, ok)
		}
	}
}
