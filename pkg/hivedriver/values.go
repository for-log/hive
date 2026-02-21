package hivedriver

import (
	"database/sql/driver"
	"fmt"

	"hive/gen/hivepb"
)

func goValueToProto(v driver.Value) (*hivepb.Value, error) {
	if v == nil {
		return &hivepb.Value{Kind: &hivepb.Value_NullValue{NullValue: true}}, nil
	}
	switch val := v.(type) {
	case int64:
		return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: val}}, nil
	case float64:
		return &hivepb.Value{Kind: &hivepb.Value_RealValue{RealValue: val}}, nil
	case string:
		return &hivepb.Value{Kind: &hivepb.Value_TextValue{TextValue: val}}, nil
	case []byte:
		return &hivepb.Value{Kind: &hivepb.Value_BlobValue{BlobValue: val}}, nil
	case bool:
		if val {
			return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: 1}}, nil
		}
		return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: 0}}, nil
	default:
		return nil, fmt.Errorf("hivedriver: unsupported value type %T", v)
	}
}

func namedValuesToProto(args []driver.NamedValue) ([]*hivepb.Value, error) {
	out := make([]*hivepb.Value, len(args))
	for i, a := range args {
		pv, err := goValueToProto(a.Value)
		if err != nil {
			return nil, err
		}
		out[i] = pv
	}
	return out, nil
}

func protoToGoValue(v *hivepb.Value) driver.Value {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *hivepb.Value_NullValue:
		if k.NullValue {
			return nil
		}
		return nil
	case *hivepb.Value_IntValue:
		return k.IntValue
	case *hivepb.Value_RealValue:
		return k.RealValue
	case *hivepb.Value_TextValue:
		return k.TextValue
	case *hivepb.Value_BlobValue:
		return k.BlobValue
	default:
		return nil
	}
}
