package dump

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MasterDumper fetches a full SQL dump from a single master.
type MasterDumper interface {
	Dump(ctx context.Context) (string, error)
}

// Aggregator collects SQL dumps from all masters and merges them into a single
// SQLite-compatible SQL script.
//
// The merge strategy is additive: tables owned by different masters are
// concatenated. If the same table appears in multiple masters (e.g. during a
// migration) the last dump wins for that table — acceptable because the
// orchestrator guarantees each table is written by exactly one master at a time.
type Aggregator struct {
	dumpers []MasterDumper
}

// New creates an Aggregator backed by the provided dumpers (one per master).
func New(dumpers []MasterDumper) *Aggregator {
	return &Aggregator{dumpers: dumpers}
}

type dumpResult struct {
	idx  int
	body string
	err  error
}

// Dump fetches dumps from all masters concurrently and merges them.
// If any master fails, an error is returned and no partial dump is produced.
func (a *Aggregator) Dump(ctx context.Context) (string, error) {
	results := make([]dumpResult, len(a.dumpers))
	var wg sync.WaitGroup

	for i, d := range a.dumpers {
		wg.Add(1)
		go func(idx int, dumper MasterDumper) {
			defer wg.Done()
			body, err := dumper.Dump(ctx)
			results[idx] = dumpResult{idx: idx, body: body, err: err}
		}(i, d)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return "", fmt.Errorf("dump: master[%d]: %w", r.idx, r.err)
		}
	}

	return merge(results), nil
}

func merge(results []dumpResult) string {
	var sb strings.Builder
	sb.WriteString("-- hive orchestrator dump\n")
	sb.WriteString("PRAGMA foreign_keys=OFF;\n\n")

	for _, r := range results {
		if r.body == "" {
			continue
		}
		fmt.Fprintf(&sb, "-- master[%d]\n", r.idx)
		// Strip PRAGMA foreign_keys lines from the individual dump to avoid
		// duplication; we emit a single pair around the whole combined dump.
		body := stripPragmaFK(r.body)
		sb.WriteString(body)
		if !strings.HasSuffix(body, "\n") {
			sb.WriteByte('\n')
		}
		sb.WriteByte('\n')
	}

	sb.WriteString("PRAGMA foreign_keys=ON;\n")
	return sb.String()
}

// stripPragmaFK removes PRAGMA foreign_keys=... lines from a SQL dump body.
func stripPragmaFK(body string) string {
	lines := strings.Split(body, "\n")
	out := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "PRAGMA FOREIGN_KEYS") {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}
