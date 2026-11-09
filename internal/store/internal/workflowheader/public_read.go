package workflowheader

import (
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
)

const PublicReadColumns = `CAST(fi.run_id AS TEXT), CAST(es.entity_id AS TEXT),
	es.flow_instance, es.entity_type, es.slug, es.name, es.fields, es.created_at, es.updated_at,
	CAST(fi.entity_id AS TEXT), fi.instance_path, fi.entity_type, fi.flow_template,
	fi.mode, fi.status, fi.config, fi.current_state, fi.stage_defined, fi.revision,
	fi.entered_state_at, fi.created_at, fi.updated_at, fi.terminated_at,
	fi.bookkeeping, fi.accumulator, fi.parent_instance, fi.instance_key, fi.life,
	(SELECT COUNT(*) FROM entity_state owner
	 WHERE owner.run_id = fi.run_id AND (owner.entity_id = fi.entity_id OR owner.flow_instance = fi.instance_path))`

// PublicReadFrom selects constructed instances, not imported data-only rows.
// The OR join exposes crossed companion identity/path owners to validation.
const PublicReadFrom = `FROM flow_instances fi LEFT JOIN entity_state es
	ON es.run_id = fi.run_id AND (es.entity_id = fi.entity_id OR es.flow_instance = fi.instance_path)`

func ScanPublic(row dataScanner, decodeTime func(any) (time.Time, bool, error), tail ...any) (operatorread.OperatorEntityFull, error) {
	return scanData(row, decodeTime, true, tail...)
}
