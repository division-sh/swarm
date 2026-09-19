package startupownership

import (
	"errors"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *postgresSession) FanOutServingCapacity() (runtimestartupownership.FanOutCapacity, error) {
	if s == nil || s.owner == nil || s.owner.backend == nil {
		return runtimestartupownership.FanOutCapacity{}, errors.New("fan-out capacity requires the retained PostgreSQL session")
	}
	maximum, dedicated, err := s.owner.backend.ConnectionCapacity()
	if err != nil {
		return runtimestartupownership.FanOutCapacity{}, err
	}
	return runtimestartupownership.PostgreSQLFanOutCapacity(maximum, dedicated)
}

func (s *sqliteSession) FanOutServingCapacity() (runtimestartupownership.FanOutCapacity, error) {
	if s == nil || s.owner == nil || s.possession == nil {
		return runtimestartupownership.FanOutCapacity{}, errors.New("fan-out capacity requires the retained SQLite owner")
	}
	return runtimestartupownership.SQLiteFanOutCapacity(), nil
}
