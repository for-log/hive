package hrana

import (
	"context"
	"fmt"
	"sort"
)

func commitAllMasters(ctx context.Context, pool MasterClientPool, batons map[int]string, idxs []int, endSQL string, afterOK func(idx int)) (*StreamResult, error) {
	sort.Ints(idxs)
	var firstResult *StreamResult
	for _, idx := range idxs {
		resp, err := pool.ClientFor(idx).Pipeline(ctx, &PipelineRequest{
			Baton: batons[idx],
			Requests: []StreamRequest{
				ExecuteRequest(endSQL, false),
				CloseRequest(),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("commit master[%d]: %w", idx, err)
		}
		if len(resp.Results) == 0 || resp.Results[0].Type != "ok" {
			msg := "upstream error"
			if len(resp.Results) > 0 && resp.Results[0].Error != nil {
				msg = resp.Results[0].Error.Message
			}
			return nil, fmt.Errorf("commit master[%d]: %s", idx, msg)
		}
		batons[idx] = resp.Baton
		if afterOK != nil {
			afterOK(idx)
		}
		if firstResult == nil {
			r := resp.Results[0]
			firstResult = &r
		}
	}
	return firstResult, nil
}

func rollbackAllMasters(ctx context.Context, pool MasterClientPool, batons map[int]string, idxs []int) (*StreamResult, error) {
	sort.Ints(idxs)
	var firstResult *StreamResult
	for _, idx := range idxs {
		resp, err := pool.ClientFor(idx).Pipeline(ctx, &PipelineRequest{
			Baton: batons[idx],
			Requests: []StreamRequest{
				ExecuteRequest("ROLLBACK", false),
				CloseRequest(),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("rollback master[%d]: %w", idx, err)
		}
		if len(resp.Results) == 0 || resp.Results[0].Type != "ok" {
			msg := "upstream error"
			if len(resp.Results) > 0 && resp.Results[0].Error != nil {
				msg = resp.Results[0].Error.Message
			}
			return nil, fmt.Errorf("rollback master[%d]: %s", idx, msg)
		}
		batons[idx] = resp.Baton
		if firstResult == nil {
			r := resp.Results[0]
			firstResult = &r
		}
	}
	return firstResult, nil
}
