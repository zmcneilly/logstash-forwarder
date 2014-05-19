package lsf

import (
	"encoding/json"
	"testing"
)

func TestJSONLoading(t *testing.T) {
	var f FileConfig
	err := json.Unmarshal([]byte("{ \"paths\": [ \"/var/log/fail2ban.log\" ], \"fields\": { \"type\": \"fail2ban\" } }"), &f)

	if err != nil {
		t.Fatalf("json.Unmarshal failed")
	}

	path_cnt_exp := 1
	if len(f.Paths) != path_cnt_exp {
		t.Fatalf("f.Paths != %d", path_cnt_exp)
	}

	path_expected := "/var/log/fail2ban.log"
	if f.Paths[0] != path_expected {
		t.Fatalf("f.Paths[0] != %s", path_expected)
	}

	type_expected := "fail2ban"
	if f.Fields["type"] != type_expected {
		t.Fatalf("f.Fields[\"type\"] != %s", type_expected)
	}
}
