package sdk

import "testing"

func TestParseProducerSession(t *testing.T) {
	session, err := parseProducerSession("OK transactional_id=tx-1 producerId=txn-abcd epoch=7")
	if err != nil {
		t.Fatalf("parseProducerSession failed: %v", err)
	}
	if session.TransactionalID != "tx-1" || session.ProducerID != "txn-abcd" || session.Epoch != 7 {
		t.Fatalf("unexpected session: %+v", session)
	}
}

func TestParseProducerSessionRejectsMissingFields(t *testing.T) {
	if _, err := parseProducerSession("OK transactional_id=tx-1 epoch=7"); err == nil {
		t.Fatal("expected missing producer id to fail")
	}
	if _, err := parseProducerSession("OK transactional_id=tx-1 producerId=p1 epoch=bad"); err == nil {
		t.Fatal("expected invalid epoch to fail")
	}
}
