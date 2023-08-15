package memory

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/eclipse/paho.golang/packets"
)

// TestMemoryStore basic tests of the memory store
func TestMemoryStore(t *testing.T) {
	s := New()

	ids := []uint16{65535, 2, 10, 32300, 5890}
	for _, id := range ids {
		pcp := packets.NewControlPacket(packets.PUBLISH)
		pcp.Content.(*packets.Publish).PacketID = id
		pcp.Content.(*packets.Publish).Payload = []byte(fmt.Sprintf("%d", id))

		if err := s.Put(pcp); err != nil {
			t.Fatalf("failed to put: %s", err)
		}
	}

	if err := s.Delete(ids[2]); err != nil {
		t.Fatalf("failed to delete: %s", err)
	}

	if _, err := s.Get(8); !errors.Is(err, ErrNotInStore) {
		t.Fatal("getting missing item should fail")
	}
	if _, err := s.Get(ids[2]); !errors.Is(err, ErrNotInStore) {
		t.Fatal("getting deleted item should fail")
	}
	ids = append(ids[:2], ids[3:]...) // keep our record in sync following delete

	if p, err := s.Get(32300); err != nil {
		t.Fatalf("failed to get: %s", err)
	} else {
		if p.PacketID() != 32300 {
			t.Fatalf("unexpected packet id returned: %s", err)
		}
		payload := p.Content.(*packets.Publish).Payload
		if bytes.Compare(payload, []byte(fmt.Sprintf("%d", 32300))) != 0 {
			t.Fatalf("unexpected payload returned: %s", payload)
		}
	}
	rids, err := s.List()
	if err != nil {
		t.Fatalf("failed to list: %s", err)
	}
	if len(rids) != len(ids) {
		t.Fatalf("List returned %d elements, expected %d", len(rids), len(ids))
	}
	for i, v := range rids {
		if v != ids[i] {
			t.Fatalf("List returned %v, expected %v", rids, ids)
		}
	}

	s.Reset()
	rids, err = s.List()
	if err != nil {
		t.Fatalf("failed to list: %s", err)
	}
	if len(rids) != 0 {
		t.Fatalf("reset did not clear store: %d", len(rids))
	}

}

// TestMemoryStoreBig creates a fully populated Store and checks things work
// Adding messages would make the structure bigger but should have no impact on the struct functions.
func TestMemoryStoreBig(t *testing.T) {
	s := New()

	for id := uint16(1); id != 0; id++ {
		pcp := packets.NewControlPacket(packets.PUBLISH)
		pcp.Content.(*packets.Publish).PacketID = id
		pcp.Content.(*packets.Publish).Payload = []byte(fmt.Sprintf("%d", id))

		if err := s.Put(pcp); err != nil {
			t.Fatalf("failed to put: %s", err)
		}
	}

	for id := uint16(1); id != 0; id++ {
		pcp, err := s.Get(id)
		if err != nil {
			t.Fatal("getting missing item should fail")
		}
		sId := pcp.Content.(*packets.Publish).PacketID
		if id != sId {
			t.Fatalf("expected %d, gor %d", id, sId)
		}
		sPayload := pcp.Content.(*packets.Publish).Payload
		expPayload := []byte(fmt.Sprintf("%d", id))
		if bytes.Compare(expPayload, sPayload) != 0 {
			t.Fatalf("expected %v, got %v", expPayload, sPayload)
		}
	}
	for id := uint16(1); id != 0; id++ {
		if err := s.Delete(id); err != nil {
			t.Fatalf("delete failed: %s", err)
		}
	}
	rids, err := s.List()
	if err != nil {
		t.Fatalf("failed to list: %s", err)
	}
	if len(rids) != 0 {
		t.Fatalf("everything should have been deleted")
	}
}
