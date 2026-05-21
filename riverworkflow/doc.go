// Package riverworkflow provides fan-out / fan-in workflow DAGs on top of
// [github.com/riverqueue/river]. A workflow is a set of named tasks with
// declared dependencies; tasks become eligible to run only after their
// dependencies complete successfully.
//
// See the README for a usage walkthrough and the godoc on
// [Client.NewWorkflow] for the API entry point.
package riverworkflow
