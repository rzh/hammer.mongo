package stats

import "testing"

func Test_getShardName(t *testing.T) {

	s := getShardName(1)
	if s != "shard0001" {
		t.Error("Name for shard 1 shall be shard0001, got %s\n", s)
	}

	s = getShardName(101)
	if s != "shard0101" {
		t.Error("Name for shard 101 shall be shard0101, got %s\n", s)
	}

}

func Test_getShardNumber(t *testing.T) {

	s := getShardNumber("shard0000")
	if s != 0 {
		t.Error("Name for shard 0 shall be 0, got %d\n", s)
	}

	s = getShardNumber("shard0001")
	if s != 1 {
		t.Error("Name for shard 1 shall be 1, got %d\n", s)
	}

	s = getShardNumber("shard0101")
	if s != 101 {
		t.Error("Name for shard 101 shall be 101, got %d\n", s)
	}

	s = getShardNumber("shard2101")
	if s != 2101 {
		t.Error("Name for shard 101 shall be 2101, got %d\n", s)
	}
}
