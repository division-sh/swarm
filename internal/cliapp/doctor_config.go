package cliapp

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type doctorConfigSource struct {
	Source  string `json:"source"`
	Path    string `json:"path,omitempty"`
	Present *bool  `json:"present"`
}

func addDoctorConfigReadback(report *LocalPreflightReport, repo string, loaded RuntimeConfigLoadResult) {
	report.ConfigSource = &doctorConfigSource{
		Source: loaded.Source, Path: loaded.Path,
		Present: loaded.sourcePresent,
	}
	addUnifiedConfigDiagnosticsToReport(report, loaded.Diagnostics)
	project := filepath.Join(repo, "swarm.yaml")
	info, err := os.Lstat(project)
	if err != nil || info.IsDir() || sameDoctorConfigFile(project, loaded.Path) {
		return
	}
	report.addWithOwner(localPreflightConfigPrerequisite, "config_unread_project", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped,
		project+" is not read; configuration lives in .swarm/swarm.yaml or --config", "", unifiedConfigOwner)
}

func sameDoctorConfigFile(project, selected string) bool {
	if selected == "" {
		return false
	}
	if filepath.Clean(project) == filepath.Clean(selected) {
		return true
	}
	projectInfo, projectErr := os.Stat(project)
	selectedInfo, selectedErr := os.Stat(selected)
	return projectErr == nil && selectedErr == nil && os.SameFile(projectInfo, selectedInfo)
}

func writeDoctorConfigSourceText(out io.Writer, source *doctorConfigSource) {
	if source == nil {
		return
	}
	name := source.Path
	if name == "" {
		name = "built-in defaults"
	}
	present := "unknown"
	if source.Present != nil {
		present = yesNo(*source.Present)
	}
	fmt.Fprintf(out, "config: %s (source: %s; present: %s)\n", name, source.Source, present)
}
