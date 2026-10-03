package contracts

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFieldGrantAuthorityRetired(t *testing.T) {
	for direction, key := range map[string]string{"inputs": "reads", "outputs": "writes"} {
		for _, value := range []string{"null", "[]", "[entity.status]", "{status: text}"} {
			t.Run(direction+"/"+value, func(t *testing.T) {
				body := fmt.Sprintf("name: grant-rejection\npins:\n  %s:\n    %s: %s\n", direction, key, value)
				if _, err := admitSchemaFragment(body); err == nil || !strings.Contains(err.Error(), "schema.yaml:") {
					t.Fatalf("retired field grant admitted or source location lost: %v\n%s", err, body)
				}
				if bundle, err := loadSchemaFragment(t, body); err == nil || bundle != nil {
					t.Fatalf("retired field grant produced a bundle: %#v, error %v", bundle, err)
				}
			})
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(FlowInputPins{}), reflect.TypeOf(FlowOutputPins{}), reflect.TypeOf(WorkflowSemanticView{})} {
		for _, field := range []string{"Reads", "Writes", "flowReads", "flowWrites", "writePinOwners"} {
			if _, present := typ.FieldByName(field); present {
				t.Errorf("retired field-grant authority remains: %s.%s", typ, field)
			}
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf((*WorkflowSemanticView)(nil)), reflect.TypeOf((*WorkflowContractBundle)(nil))} {
		for _, method := range []string{"FlowReadPins", "FlowWritePins", "WritePinOwners"} {
			if _, present := typ.MethodByName(method); present {
				t.Errorf("retired field-grant accessor remains: %s.%s", typ, method)
			}
		}
	}
}
