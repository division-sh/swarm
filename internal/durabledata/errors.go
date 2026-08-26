package durabledata

import "fmt"

type ErrorCode string

const (
	CodeInvocationConflict ErrorCode = "DATA_INVOCATION_CONFLICT"
	CodeContractNotFound   ErrorCode = "DATA_CONTRACT_NOT_FOUND"
	CodeDeclarationMissing ErrorCode = "DATA_DECLARATION_NOT_FOUND"
	CodeVersionMissing     ErrorCode = "DATA_VERSION_NOT_FOUND"
	CodePayloadPruned      ErrorCode = "DATA_PAYLOAD_PRUNED"
	CodeOperationMissing   ErrorCode = "DATA_OPERATION_NOT_FOUND"
	CodeDependencyMissing  ErrorCode = "DATA_DEPENDENCY_INCOMPLETE"
	CodePinConflict        ErrorCode = "DATA_PIN_CONFLICT"
	CodeSchemaMismatch     ErrorCode = "DATA_SCHEMA_MISMATCH"
	CodeRunDataImmutable   ErrorCode = "RUN_DATA_IMMUTABLE"
	CodeRunDataRejected    ErrorCode = "RUN_DATA_REJECTED"
	CodeRunHeadConflict    ErrorCode = "RUN_DATA_HEAD_CONFLICT"
	CodeAccessDenied       ErrorCode = "DATA_ACCESS_DENIED"
	CodeIntegrity          ErrorCode = "DATA_INTEGRITY_ERROR"
)

type DomainError struct {
	Code    ErrorCode
	Message string
	Details map[string]any
}

func (e *DomainError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewDomainError(code ErrorCode, format string, args ...any) error {
	return &DomainError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func NewDomainErrorWithDetails(code ErrorCode, details map[string]any, format string, args ...any) error {
	return &DomainError{Code: code, Message: fmt.Sprintf(format, args...), Details: details}
}
