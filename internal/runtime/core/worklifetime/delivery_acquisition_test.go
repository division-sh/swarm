package worklifetime

import "testing"

func TestDeliveryAcquisitionIsClosedAndExact(t *testing.T) {
	for _, a := range []DeliveryAcquisition{
		AcquiredDelivery(&scriptedDeliveryContinuation{}),
		AlreadyOwnedDelivery("delivery-1"),
		TerminallyFencedDelivery("delivery-1"),
	} {
		if err := a.Validate("delivery-1"); err != nil {
			t.Fatal(err)
		}
		if err := a.Validate("other"); err == nil {
			t.Fatal("foreign delivery acquisition accepted")
		}
		capability, acquired := a.Acquired()
		if acquired != (a.Disposition() == DeliveryAcquired) || (capability != nil) != acquired {
			t.Fatal("contention result fabricated a capability")
		}
	}
	for _, a := range []DeliveryAcquisition{
		{}, AcquiredDelivery(nil), AlreadyOwnedDelivery(""),
		{deliveryID: "delivery-1", disposition: DeliveryAcquired},
		{deliveryID: "delivery-1", disposition: 99},
		{deliveryID: "delivery-1", disposition: DeliveryAlreadyOwned, continuation: &scriptedDeliveryContinuation{}},
	} {
		if err := a.Validate("delivery-1"); err == nil {
			t.Fatal("invalid delivery acquisition accepted")
		}
	}
}
