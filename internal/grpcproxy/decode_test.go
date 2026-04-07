package grpcproxy

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// buildQuery builds: message Query { string stmt=1; ... }
// positional bytes are included as field 2 if non-nil (opaque — not decoded).
func buildQuery(stmt string, positional []byte) []byte {
	var buf []byte
	buf = protowire.AppendTag(buf, 1, protowire.BytesType)
	buf = protowire.AppendString(buf, stmt)
	if positional != nil {
		buf = protowire.AppendTag(buf, 2, protowire.BytesType)
		buf = protowire.AppendBytes(buf, positional)
	}
	return buf
}

func buildStep(query []byte) []byte {
	var buf []byte
	buf = protowire.AppendTag(buf, 2, protowire.BytesType)
	buf = protowire.AppendBytes(buf, query)
	return buf
}

func buildProgram(steps ...[]byte) []byte {
	var buf []byte
	for _, s := range steps {
		buf = protowire.AppendTag(buf, 1, protowire.BytesType)
		buf = protowire.AppendBytes(buf, s)
	}
	return buf
}

func buildProgramReq(clientID string, program []byte) []byte {
	var buf []byte
	buf = protowire.AppendTag(buf, 1, protowire.BytesType)
	buf = protowire.AppendString(buf, clientID)
	buf = protowire.AppendTag(buf, 2, protowire.BytesType)
	buf = protowire.AppendBytes(buf, program)
	return buf
}

func TestDecodeProgramReq_SingleStatement(t *testing.T) {
	query := buildQuery("INSERT INTO users (id, name) VALUES (?1, ?2)", []byte{0x01, 0x02})
	step := buildStep(query)
	pgm := buildProgram(step)
	reqBytes := buildProgramReq("test-client", pgm)

	req, err := DecodeProgramReq(reqBytes)
	if err != nil {
		t.Fatalf("DecodeProgramReq: %v", err)
	}
	if req.ClientID != "test-client" {
		t.Errorf("ClientID = %q, want %q", req.ClientID, "test-client")
	}
	if len(req.Queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(req.Queries))
	}
	if req.Queries[0].SQL != "INSERT INTO users (id, name) VALUES (?1, ?2)" {
		t.Errorf("SQL = %q", req.Queries[0].SQL)
	}
}

func TestDecodeProgramReq_MultipleStatements(t *testing.T) {
	s1 := buildStep(buildQuery("CREATE TABLE users (id INTEGER PRIMARY KEY)", nil))
	s2 := buildStep(buildQuery("CREATE TABLE posts (id INTEGER PRIMARY KEY)", nil))
	s3 := buildStep(buildQuery("CREATE TABLE comments (id INTEGER PRIMARY KEY)", nil))
	pgm := buildProgram(s1, s2, s3)
	reqBytes := buildProgramReq("cid", pgm)

	req, err := DecodeProgramReq(reqBytes)
	if err != nil {
		t.Fatalf("DecodeProgramReq: %v", err)
	}
	if len(req.Queries) != 3 {
		t.Fatalf("queries = %d, want 3", len(req.Queries))
	}
	wantSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY)",
		"CREATE TABLE posts (id INTEGER PRIMARY KEY)",
		"CREATE TABLE comments (id INTEGER PRIMARY KEY)",
	}
	for i, want := range wantSQL {
		if req.Queries[i].SQL != want {
			t.Errorf("Queries[%d].SQL = %q, want %q", i, req.Queries[i].SQL, want)
		}
	}
}

func TestDecodeProgramReq_NoParams(t *testing.T) {
	query := buildQuery("SELECT 1", nil)
	step := buildStep(query)
	pgm := buildProgram(step)
	reqBytes := buildProgramReq("cid", pgm)

	req, err := DecodeProgramReq(reqBytes)
	if err != nil {
		t.Fatalf("DecodeProgramReq: %v", err)
	}
	if len(req.Queries) != 1 {
		t.Fatalf("queries = %d", len(req.Queries))
	}
	if req.Queries[0].SQL != "SELECT 1" {
		t.Errorf("SQL = %q, want %q", req.Queries[0].SQL, "SELECT 1")
	}
}

func TestDecodeProgramReq_SkipsOpaqueParams(t *testing.T) {
	opaqueParams := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03}
	query := buildQuery("INSERT INTO t VALUES (?1)", opaqueParams)
	step := buildStep(query)
	pgm := buildProgram(step)
	reqBytes := buildProgramReq("cid", pgm)

	req, err := DecodeProgramReq(reqBytes)
	if err != nil {
		t.Fatalf("DecodeProgramReq: %v", err)
	}
	if len(req.Queries) != 1 {
		t.Fatalf("queries = %d", len(req.Queries))
	}
	if req.Queries[0].SQL != "INSERT INTO t VALUES (?1)" {
		t.Errorf("SQL = %q", req.Queries[0].SQL)
	}
}

func TestDecodeProgramReq_EmptyProgram(t *testing.T) {
	pgm := buildProgram()
	reqBytes := buildProgramReq("cid", pgm)

	req, err := DecodeProgramReq(reqBytes)
	if err != nil {
		t.Fatalf("DecodeProgramReq: %v", err)
	}
	if len(req.Queries) != 0 {
		t.Errorf("queries = %d, want 0", len(req.Queries))
	}
}
