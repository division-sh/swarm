package llmpersistence

func rotationReceiptValue(value string, keyed bool) any {
	if !keyed {
		return nil
	}
	return value
}
