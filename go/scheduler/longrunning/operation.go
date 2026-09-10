// Package longrunning serves google.longrunning.Operations (AIP-151) over the
// scheduler: an operation is a scheduler job seen through its method's
// contract, named `{job parent}/operations/{job}`. It holds the three pieces
// a service returning Operations needs: the embeddable Operations server, the
// producer that hands a request to the scheduler as a job, and the runner-side
// helpers the scheduler's callback uses to report progress and hand back the
// outcome.
package longrunning

import (
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"go.einride.tech/aip/resourcename"

	"github.com/malonaz/core/gengo/scheduler/model"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

// operationsCollection is the collection operations hang off their parent
// under, sibling to the jobs collection.
const operationsCollection = "operations"

// JobParentOf returns the scheduler parent of a job acting on the resource,
// which is where its operation lives: the user when the resource is a user's
// (`organizations/{o}/users/{u}/...`), else the organization when the resource
// is an organization's, else the root. Only a `users` collection directly under
// the organization makes a user parent.
func JobParentOf(resource string) string {
	if parent, ok := resourcename.Ancestor(resource, schedulerpb.UserResourceName{}.Pattern()); ok {
		return parent
	}
	if parent, ok := resourcename.Ancestor(resource, schedulerpb.OrganizationResourceName{}.Pattern()); ok {
		return parent
	}
	return ""
}

// OperationName returns the name of the operation the job backs: the job's
// name with `jobs` read as `operations`.
func OperationName(jobName string) (string, error) {
	organizationID, userID, jobID, err := model.ParseJobName(jobName)
	if err != nil {
		return "", err
	}
	return resourcename.Join(jobParent(organizationID, userID), operationsCollection, jobID), nil
}

// jobParent assembles a job parent from its IDs.
func jobParent(organizationID, userID string) string {
	switch {
	case userID != "":
		return (&schedulerpb.UserResourceName{Organization: organizationID, User: userID}).String()
	case organizationID != "":
		return (&schedulerpb.OrganizationResourceName{Organization: organizationID}).String()
	}
	return ""
}

// jobNameOf returns the name of the job backing the operation. Returns false
// when the name is not an operation name: `{job parent}/operations/{id}`.
func jobNameOf(operationName string) (string, bool) {
	parent, operationID, ok := splitOperationName(operationName)
	if !ok {
		return "", false
	}
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

// splitOperationName splits an operation name into its parent and ID.
func splitOperationName(operationName string) (parent, operationID string, ok bool) {
	segments := strings.Split(operationName, "/")
	if len(segments) < 2 || segments[len(segments)-2] != operationsCollection || segments[len(segments)-1] == "" {
		return "", "", false
	}
	return strings.Join(segments[:len(segments)-2], "/"), segments[len(segments)-1], true
}

// OperationFromJob projects a job onto the operation it backs: done once the
// job is terminal, carrying the job's response or error, and the job's latest
// progress as metadata. A cancelled job is a done operation with a CANCELLED error.
func OperationFromJob(job *schedulerpb.Job) (*longrunningpb.Operation, error) {
	name, err := OperationName(job.GetName())
	if err != nil {
		return nil, err
	}
	operation := &longrunningpb.Operation{
		Name:     name,
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
	return operation, nil
}
