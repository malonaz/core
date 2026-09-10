// Package longrunning serves google.longrunning.Operations (AIP-151) over the
// scheduler: an operation is a scheduler job whose `operation_name` is set, and
// whose ID is the job's ID. It holds the three pieces a service returning
// Operations needs: the embeddable Operations server, the producer that hands
// a request to the scheduler as a job, and the runner-side helpers the
// scheduler's callback uses to report progress and hand back the outcome.
package longrunning

import (
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"go.einride.tech/aip/resourcename"

	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

// operationsCollection is the collection operations hang off their resource under.
const operationsCollection = "operations"

// JobParentOf returns the scheduler parent of a job backing an operation on the
// resource: the user when the resource is a user's (`organizations/{o}/users/{u}/...`),
// else the organization when the resource is an organization's, else the root.
// Only a `users` collection directly under the organization makes a user parent.
func JobParentOf(resource string) string {
	if parent, ok := resourcename.Ancestor(resource, schedulerpb.UserResourceName{}.Pattern()); ok {
		return parent
	}
	if parent, ok := resourcename.Ancestor(resource, schedulerpb.OrganizationResourceName{}.Pattern()); ok {
		return parent
	}
	return ""
}

// OperationName returns the name of the operation with the given ID on the resource.
func OperationName(resource, operationID string) string {
	return resourcename.Join(resource, operationsCollection, operationID)
}

// jobNameOf returns the name of the job backing the operation, which follows
// from the operation's name alone: the job's parent is derived from the
// resource and its ID is the operation's. Returns false when the name is not
// an operation name.
func jobNameOf(operationName string) (string, bool) {
	separator := "/" + operationsCollection + "/"
	index := strings.LastIndex(operationName, separator)
	if index < 0 {
		return "", false
	}
	resource, operationID := operationName[:index], operationName[index+len(separator):]
	if resource == "" || operationID == "" || strings.Contains(operationID, "/") {
		return "", false
	}
	parent := JobParentOf(resource)
	var user schedulerpb.UserResourceName
	var organization schedulerpb.OrganizationResourceName
	switch {
	case parent == "":
		return schedulerpb.JobResourceName{Job: operationID}.String(), true
	case user.UnmarshalString(parent) == nil:
		return user.OrganizationsUsersJobResourceName(operationID).String(), true
	case organization.UnmarshalString(parent) == nil:
		return organization.OrganizationsJobResourceName(operationID).String(), true
	}
	return "", false
}

// OperationFromJob projects a job onto the operation it backs: done once the
// job is terminal, carrying the job's response or error, and the job's latest
// progress as metadata. A cancelled job is a done operation with a CANCELLED error.
func OperationFromJob(job *schedulerpb.Job) *longrunningpb.Operation {
	operation := &longrunningpb.Operation{
		Name:     job.GetOperationName(),
		Metadata: job.GetProgress(),
	}
	switch job.GetState() {
	case schedulerpb.JobState_JOB_STATE_SUCCEEDED:
		operation.Done = true
		if job.GetResponse() != nil {
			operation.Result = &longrunningpb.Operation_Response{Response: job.GetResponse()}
		}
	case schedulerpb.JobState_JOB_STATE_FAILED, schedulerpb.JobState_JOB_STATE_CANCELLED:
		operation.Done = true
		operation.Result = &longrunningpb.Operation_Error{Error: job.GetError()}
	}
	return operation
}
