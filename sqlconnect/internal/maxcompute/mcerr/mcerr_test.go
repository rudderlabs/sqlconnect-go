package mcerr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Fake ids: the verbatim texts come from findings 10 and 15 and E34/E31, with
// every id replaced.
const (
	fakeKeyID     = "LTAI5tFakeKeyId0123456789"
	fakePlainID   = "rsretlprobefakeaccesskeyid"
	fakeSTSID     = "STS.NUFakeStsKeyId0123456789"
	fakeContextID = "57a27139-aeca-4e40-8eb7-ae4d5f9636e5"
	fakePrincipal = "RAM$1234567890123456:role/rudderstack-mc-min-access"
)

func httpErr(status int, body string) error {
	e := restclient.HttpError{StatusCode: status, Status: fmt.Sprint(status), RequestId: "6AFD9F42E2DC900100493F4B", Body: []byte(body), ErrorMessage: restclient.NewErrorMessage([]byte(body))}
	return pkgerrors.WithStack(e)
}

func xmlError(code, message string) string {
	return `<?xml version="1.0" encoding="UTF-8"?> <Error> <Code>` + code + `</Code> <Message><![CDATA[` + message + `]]></Message> <RequestId>6AFD9F42E2DC900100493F4B</RequestId> <HostId>odps</HostId> </Error>`
}

func TestClassifyAndRedact(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		category string
		code     string
		retry    bool
	}{
		{"wrong secret", httpErr(403, xmlError("SignatureNotMatch", "ODPS-0410042:Invalid signature value - User signature does not match.")), CategoryAuth, "SignatureNotMatch", false},
		{"unknown key id", httpErr(403, xmlError("AccessKeyIdNotFound", "ODPS-0410051:Invalid credentials - accessKeyId not found: "+fakePlainID)), CategoryAuth, "AccessKeyIdNotFound", false},
		{"expired sts token", httpErr(403, xmlError("AccessKeyIdNotFound", "ODPS-0410051:Invalid credentials - accessKeyId not found: "+fakeSTSID)), CategoryAuth, "AccessKeyIdNotFound", false},
		{"tampered sts token", httpErr(400, xmlError("InvalidParameter", "ODPS-0410051:Invalid credentials - AccessKeyId '"+fakeSTSID+"' is invalid.")), CategoryAuth, "InvalidParameter", false},
		{"no describe", httpErr(403, xmlError("NoPermission", "ODPS-0420095: Access Denied - Authorization Failed [4009,4019], You have NO privilege 'odps:Describe' on {acs:odps:*:projects/p/schemas/s/tables/t}. Deny as default for Resource. Context ID:"+fakeContextID+". --->Tips: Principal:"+fakePrincipal+";")), CategoryPermission, "NoPermission", false},
		{"wrong region", httpErr(404, xmlError("NoSuchObject", "ODPS-0420111: Project not found - 'customer_project'.")), CategoryProjectNotFound, "ODPS-0420111", false},
		{"missing schema over REST", httpErr(404, xmlError("NoSuchObject", "0420111:Database not found - Schema s does not exist")), CategoryNotFound, "ODPS-0420111", false},
		{"missing table over REST", httpErr(404, xmlError("NoSuchObject", "ODPS-0130131:Table not found - 'p.t' table not found")), CategoryNotFound, "ODPS-0130131", false},
		{"endpoint without /api", httpErr(404, `<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN"> <html> <head><title>404 Not Found</title></head> <body> <td>https://service.us-west-1.maxcompute.aliyun.com/projects/p/instances</td> <td>odps-frontend-server-026078058253.oc27-vpc</td></body></html>`), CategoryBadEndpoint, "HTTP-404", false},
		{"parse error at submit", httpErr(400, xmlError("ParseError", "ODPS-0130161:[1,1] Parse exception - invalid token 'selec'")), CategorySyntax, "ODPS-0130161", false},
		{"throttled", httpErr(429, xmlError("Throttled", "slow down")), CategoryUnknown, "HTTP-429", true},
		{"mcqa", httpErr(400, xmlError("InvalidParameter", "ODPS-0420061: Cannot find MCQA quota to start connection on project p")), CategoryInternal, "ODPS-0420061", false},
		{"dns", &url.Error{Op: "Post", URL: "https://service.us-west-9.maxcompute.aliyun.com/api/projects/p/instances", Err: &net.DNSError{Err: "no such host", Name: "service.us-west-9.maxcompute.aliyun.com", IsNotFound: true}}, CategoryNetwork, "", true},
		{"bare EOF", pkgerrors.WithStack(io.EOF), CategoryNetwork, "", true},
		{"unexpected EOF", pkgerrors.WithStack(io.ErrUnexpectedEOF), CategoryNetwork, "", true},
		{"reset", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, CategoryNetwork, "", true},
		{"job: missing table", errors.New("ODPS-0130131:[1,15] Table not found - table p.s.t cannot be resolved"), CategoryNotFound, "ODPS-0130131", false},
		{"job: no select", errors.New("ODPS-0130013:Authorization exception - Authorization Failed [4009,4019], You have NO privilege 'odps:Select' on {acs:odps:*:projects/p/schemas/s/tables/t}. Deny as default for Resource. Context ID:" + fakeContextID + ".\t--->Tips: CurrentProject:p; Principal:" + fakePrincipal + ";"), CategoryPermission, "ODPS-0130013", false},
		{"job: full scan", errors.New("ODPS-0130071:[1,22] Semantic analysis exception - physical plan generation failed: Table(p,s,t) is full scan with all partitions, please specify partition predicates."), CategoryFullScan, "ODPS-0130071", false},
		{"job: order by", errors.New("ODPS-0130071:[1,30] Semantic analysis exception - ORDER BY must be used with a LIMIT clause, please set odps.sql.validate.orderby.limit=false to use it."), CategoryInternal, "ODPS-0130071", false},
		{"job: column", errors.New("ODPS-0130071:[1,8] Semantic analysis exception - column nope cannot be resolved"), CategorySemantic, "ODPS-0130071", false},
		{"job: cast", errors.New("ODPS-0130141:[1,8] Illegal implicit type cast - cannot cast from ARRAY<INT> to STRING"), CategorySemantic, "ODPS-0130141", false},
		{"job: union", errors.New("ODPS-0130241:[1,32] Illegal union operation - type mismatch for column 0 of UNION"), CategorySemantic, "ODPS-0130241", false},
		{"job: exists", errors.New("ODPS-0130211:[1,14] Table or view already exists - table or view p.s.t is already defined"), CategoryAlreadyExists, "ODPS-0130211", false},
		{"job: overflow", errors.New("ODPS-0130071:[0,0] Semantic analysis exception - physical plan generation failed: SQL Runtime Unretryable Error: ODPS-0121145:Data overflow - 1 / 0"), CategoryData, "ODPS-0121145", false},
		{"job: runtime regex", errors.New("ODPS-0121011: Invalid regular expression pattern - in function REGEXP_REPLACE, regular expression call failed"), CategoryData, "ODPS-0121011", false},
		{"job: cancelled", errors.New("ODPS-0424171: Instance is cancelled - 20261118124958506g4yzs7rdtm84"), CategoryCancelled, "ODPS-0424171", false},
		{"job: internal", errors.New("ODPS-0010000:System internal error - Null data task in Plan"), CategoryUnknown, "ODPS-0010000", true},
		{"job: debug tail", errors.New("ODPS-0420111: Project not found - 'p'.\nDebugingInfo amF2YS5sYW5nLlJ1bnRpbWVFeGNlcHRpb246IE9EUFMtMDQyMDExMTog"), CategoryProjectNotFound, "ODPS-0420111", false},
		{"job: debug tail on one line", errors.New("ODPS-0420111: Project not found - 'p'. DebugingInfo amF2YS5sYW5nLlJ1bnRpbWVFeGNlcHRpb246IE9EUFMtMDQyMDExMTog"), CategoryProjectNotFound, "ODPS-0420111", false},
		{"multi statement", errors.New(`Please add put { "odps.sql.submit.mode" : "script"} for multi-statement query in settings.`), CategoryRefused, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := FromSDK(c.err)
			require.Equal(t, c.category, e.Category, e.Error())
			require.Equal(t, c.code, e.Code)
			require.Equal(t, c.retry, e.Retryable)
			text := e.Error()
			for _, secret := range []string{fakeKeyID, fakePlainID, fakeSTSID, "NUFakeStsKeyId", fakeContextID, "1234567890123456", "rudderstack-mc-min-access", "DebugingInfo", "amF2YS5sYW5n", "https://", "<html"} {
				require.NotContains(t, text, secret)
			}
			require.LessOrEqual(t, len([]rune(text)), 600)
		})
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"accessKeyId not found: " + fakeSTSID:                  "accessKeyId not found: [credential]",
		"AccessKeyId '" + fakePlainID + "' is invalid.":        "AccessKeyId '[credential]' is invalid.",
		"accessKeyId=<redacted> found: " + fakePlainID:         "[credential] found: [credential]",
		"token " + fakeSTSID + " expired":                      "token [credential] expired",
		"Deny as default. Context ID:" + fakeContextID + ".":   "Deny as default. Context ID:[redacted].",
		"principal " + fakePrincipal + " denied":               "principal [principal] denied",
		"AccessKeyId \"" + fakePlainID + "\" is invalid.":      "AccessKeyId \"[credential]\" is invalid.",
		"principal ALIYUN$owner@example.com denied":            "principal [principal] denied",
		"text --->Tips: Principal:" + fakePrincipal:            "text",
		"ODPS-0420111: Project not found DebugingInfo abcdefg": "ODPS-0420111: Project not found",
	} {
		require.Equal(t, want, Redact(in))
	}
	require.Len(t, []rune(Redact(strings.Repeat("中", 600))), 512)
}

func TestContextAndSubmit(t *testing.T) {
	e := Context(context.Canceled, "inst", "terminated")
	require.ErrorIs(t, e, context.Canceled)
	require.Equal(t, CategoryCancelled, e.Category)

	dial := FromSubmit(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
	require.False(t, dial.OutcomeUnknown, "a dial failure sent nothing")
	require.True(t, dial.Retryable)
	lost := FromSubmit(&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")})
	require.True(t, lost.OutcomeUnknown, "a lost response may have created the job")
	require.False(t, lost.Retryable)
	require.EqualValues(t, 130131, ServerCode("ODPS-0130131"))

	timeout := FromSDK(&url.Error{Op: "Get", URL: "https://x", Err: timeoutErr{}})
	require.Contains(t, timeout.Message, "timed out", "a timeout keeps its diagnostic")

	local := Local(CategoryRefused, "invalid relation name https://x/"+fakeSTSID+" "+strings.Repeat("a", 600))
	require.NotContains(t, local.Error(), fakeSTSID)
	require.NotContains(t, local.Error(), "https://")
	require.LessOrEqual(t, len([]rune(local.Message)), 512)
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClientTimeoutIsNotACallerCancel(t *testing.T) {
	clientTimeout := &url.Error{Op: "Post", URL: "https://x", Err: fmt.Errorf("%w (Client.Timeout exceeded while awaiting headers)", context.DeadlineExceeded)}
	require.ErrorIs(t, clientTimeout, context.DeadlineExceeded, "what net/http returns")
	e := FromSubmit(pkgerrors.WithStack(clientTimeout))
	require.Equal(t, CategoryNetwork, e.Category)
	require.True(t, e.OutcomeUnknown)
	require.NotErrorIs(t, e, context.DeadlineExceeded)

	require.Equal(t, CategoryCancelled, FromSDK(context.DeadlineExceeded).Category)
	require.Equal(t, CategoryCancelled, FromSDK(pkgerrors.WithStack(context.Canceled)).Category)
}

// SafeError rewrites "accessKeyId=…" and cuts a RAM principal at "/", so the
// key-id echo and principal patterns must run on the raw text first.
func TestSafeRedactsBeforeSafeError(t *testing.T) {
	echo := "ODPS-0410051:Invalid credentials - accessKeyId=abc found: " + fakePlainID
	principal := "ODPS-0420095: Access Denied - principal " + fakePrincipal + " denied"
	for name, err := range map[string]error{
		"http key-id echo":     httpErr(403, xmlError("AccessKeyIdNotFound", echo)),
		"plain key-id echo":    errors.New(echo),
		"http principal":       httpErr(403, xmlError("NoPermission", principal)),
		"plain principal":      errors.New(principal),
		"double-quoted key id": errors.New("ODPS-0410051:Invalid credentials - AccessKeyId \"" + fakePlainID + "\" is invalid."),
		"aliyun principal":     errors.New("ODPS-0420095: Access Denied - principal ALIYUN$owner@example.com denied"),
		"http body principal": func() error {
			e := restclient.HttpError{StatusCode: 403, Body: []byte(xmlError("NoPermission", principal))}
			return pkgerrors.WithStack(e)
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			for _, e := range []*Error{FromSDK(err), FromSubmit(err), FromJob(err.Error())} {
				text := e.Error()
				for _, secret := range []string{fakePlainID, "1234567890123456", "rudderstack-mc-min-access", "owner@example.com"} {
					require.NotContains(t, text, secret)
				}
			}
		})
	}
}
