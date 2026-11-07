package events

import "fmt"

// InstanceOwnership is an owner-observed continuation fact, not execution
// authority. Its zero value is missing evidence, never a run-owned statement.
type InstanceOwnership struct {
	RunOwned bool          `json:"run_owned,omitempty"`
	Life     *InstanceLife `json:"instance_life,omitempty"`
}

func (o InstanceOwnership) Validate() error {
	if o.RunOwned {
		if o.Life != nil {
			return fmt.Errorf("run-owned work cannot carry an instance life")
		}
		return nil
	}
	if o.Life == nil {
		return fmt.Errorf("work ownership requires its explicit owner observation")
	}
	return o.Life.Validate()
}

func (o InstanceOwnership) Equal(other InstanceOwnership) bool {
	if o.Validate() != nil || other.Validate() != nil || o.RunOwned != other.RunOwned {
		return false
	}
	return o.RunOwned || *o.Life == *other.Life
}

func (o InstanceOwnership) Clone() InstanceOwnership {
	if o.Life != nil {
		life := *o.Life
		o.Life = &life
	}
	return o
}
