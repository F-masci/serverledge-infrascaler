package test

import (
	"context"
	"encoding/json"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/utils"
)

// TestNearestNeighborsContainOnlyRegisteredNodes covers #10 via real discovery
// and UDP status responses. With exactly two known peers, the two selected keys
// must identify those peers; zero-filled buffer entries used to produce empty keys.
func TestNearestNeighborsContainOnlyRegisteredNodes(t *testing.T) {
	requirePlatformProfile(t, "discovery")
	area := registration.SelfRegistration.Area
	for index, name := range []string{"near", "far"} {
		peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(HOST)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { peer.Close() })
		coordinateConfig := vivaldi.DefaultConfig()
		coordinateConfig.Dimensionality = 3
		coordinate := vivaldi.NewCoordinate(coordinateConfig)
		coordinate.Vec[0] = float64(1 + index*9)
		payload, err := json.Marshal(registration.StatusInformation{Coordinates: *coordinate, TotalCPU: 2, TotalMemory: 1024, AvailableMemory: 1024})
		if err != nil {
			t.Fatal(err)
		}
		// Each fixture answers the same one-byte status protocol as a real peer.
		go func() {
			buffer := make([]byte, 32)
			for {
				_, address, err := peer.ReadFromUDP(buffer)
				if err != nil {
					return
				}
				peer.WriteToUDP(payload, address)
			}
		}()
		key := publishTestPeer(t, area, name, "pool", runtime.GOARCH, HOST+":1", peer.LocalAddr().(*net.UDPAddr).Port)
		// Remove discovery membership before closing the UDP responder, so the
		// shared monitor can finish polling this peer instead of waiting on it.
		t.Cleanup(func() {
			client, err := utils.GetEtcdClient()
			if err != nil {
				t.Error(err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := client.Delete(ctx, key); err != nil {
				t.Error(err)
				return
			}
			waitForPlatformCondition(t, "removal of peer "+name, 4*time.Second, func() bool {
				_, found := registration.GetFullNeighborInfo()[name]
				return registration.GetPeerFromKey(name) == nil && !found
			})
		})
	}
	waitForPlatformCondition(t, "two registered nearest neighbors", 5*time.Second, func() bool {
		selected := registration.GetNearestNeighbors()
		if len(selected) != 2 {
			return false
		}
		keys := map[string]bool{}
		for _, peer := range selected {
			keys[peer.Key] = true
		}
		return keys["near"] && keys["far"]
	})
}
