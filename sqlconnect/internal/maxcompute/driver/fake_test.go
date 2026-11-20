package driver

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/require"
)

// fakeODPS answers the REST calls of the submit/poll/terminate path.
type fakeODPS struct {
	t   *testing.T
	srv *httptest.Server

	mu sync.Mutex
	// statuses are the task statuses the n-th poll returns; the last repeats.
	statuses []string
	// failureText is the job result when a task fails.
	failureText string
	// submitDelay delays the submit answer, as a hung connection does.
	submitDelay time.Duration
	// conflicts answers the first n submits with HTTP 409 and Retry-After: 1,
	// which the SDK retries on its own for up to 180 s.
	conflicts int
	// submitStatus and submitBody, when set, reject the submit.
	submitStatus int
	submitBody   string
	// syncResult, when set, answers the submit with a synchronous task result.
	syncResult string
	// terminateStatus answers terminate calls; 0 means 200.
	terminateStatus int
	// terminateBody is the error body of a refused terminate.
	terminateBody string
	// pollFailures makes the first n polls fail at the transport.
	pollFailures int
	// resultFailures and terminateFailures make the first n result reads and
	// terminate calls fail at the transport.
	resultFailures, terminateFailures int
	results                           int
	// pollDelay delays each poll answer.
	pollDelay time.Duration
	// pollStatus, when set, answers status polls with that HTTP status and
	// pollBody: the first pollStatusTimes polls, or every poll when it is 0.
	pollStatus      int
	pollStatusTimes int
	pollBody        string

	submits    []submission
	polls      int
	terminates int
	terminated bool
	// pollsAtTerminate is the poll count when the first terminate arrived.
	pollsAtTerminate int
}

type submission struct {
	Query    string
	Hints    map[string]string
	TaskName string
}

var (
	queryPattern    = regexp.MustCompile(`(?s)<Query>(.*)</Query>`)
	taskNamePattern = regexp.MustCompile(`<SQL>\s*<Name>([^<]*)</Name>`)
	settingsPattern = regexp.MustCompile(`(?s)<Name>settings</Name>\s*<Value>(.*?)</Value>`)
)

func newFakeODPS(t *testing.T) *fakeODPS {
	f := &fakeODPS{t: t, statuses: []string{"Success"}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeODPS) client() *odps.Odps {
	o := odps.NewOdps(account.NewAliyunAccount("id", "secret", "cn-test"), f.srv.URL)
	o.SetDefaultProjectName("p")
	return o
}

func (f *fakeODPS) db(t *testing.T, mutate ...func(*Config)) *sql.DB {
	cfg := Config{Client: f.client(), DefaultSchema: "s1", PollInitial: 5 * time.Millisecond, PollMax: 10 * time.Millisecond, TerminateWait: time.Second}
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (f *fakeODPS) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/projects/p/instances":
		body, _ := io.ReadAll(r.Body)
		s := submission{Hints: map[string]string{}}
		if m := queryPattern.FindSubmatch(body); m != nil {
			s.Query = html.UnescapeString(strings.TrimSuffix(strings.TrimPrefix(string(m[1]), "<![CDATA["), "]]>"))
		}
		if m := taskNamePattern.FindSubmatch(body); m != nil {
			s.TaskName = string(m[1])
		}
		if m := settingsPattern.FindSubmatch(body); m != nil {
			require.NoError(f.t, json.Unmarshal([]byte(html.UnescapeString(string(m[1]))), &s.Hints))
		}
		f.submits = append(f.submits, s)
		if f.submitDelay > 0 {
			d := f.submitDelay
			f.mu.Unlock()
			time.Sleep(d)
			f.mu.Lock()
		}
		if f.conflicts > 0 {
			f.conflicts--
			w.Header().Set("Location", f.srv.URL+"/projects/p/instances/inst1")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>Busy</Code><Message><![CDATA[try later]]></Message></Error>`)
			return
		}
		if f.submitStatus != 0 {
			w.WriteHeader(f.submitStatus)
			_, _ = io.WriteString(w, f.submitBody)
			return
		}
		w.Header().Set("Location", f.srv.URL+"/projects/p/instances/inst1")
		if f.syncResult != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, taskResultXML("Failed", f.syncResult))
			return
		}
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodGet && r.URL.Path == "/projects/p/instances/inst1" && r.URL.Query().Has("taskstatus"):
		f.polls++
		if f.pollDelay > 0 {
			f.mu.Unlock()
			time.Sleep(f.pollDelay)
			f.mu.Lock()
		}
		if f.pollFailures > 0 {
			f.pollFailures--
			hj, ok := w.(http.Hijacker)
			require.True(f.t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(f.t, err)
			_ = conn.Close()
			return
		}
		if f.pollStatus != 0 && (f.pollStatusTimes == 0 || f.polls <= f.pollStatusTimes) {
			w.WriteHeader(f.pollStatus)
			body := f.pollBody
			if body == "" {
				body = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalServerError</Code><Message><![CDATA[ODPS-0010000:System internal error]]></Message></Error>`
			}
			_, _ = io.WriteString(w, body)
			return
		}
		status := f.statuses[min(f.polls-1, len(f.statuses)-1)]
		if f.terminated {
			status = "Cancelled"
		}
		_, _ = fmt.Fprintf(w, `<Instance><Tasks><Task Type="SQL"><Name>AnonymousSQLTask</Name><Status>%s</Status></Task></Tasks></Instance>`, status)
	case r.Method == http.MethodGet && r.URL.Path == "/projects/p/instances/inst1" && r.URL.Query().Has("result"):
		f.results++
		if f.resultFailures > 0 {
			f.resultFailures--
			f.drop(w)
			return
		}
		_, _ = io.WriteString(w, taskResultXML("Failed", f.failureText))
	case r.Method == http.MethodPut && r.URL.Path == "/projects/p/instances/inst1":
		if f.terminates == 0 {
			f.pollsAtTerminate = f.polls
		}
		f.terminates++
		if f.terminateFailures > 0 {
			f.terminateFailures--
			f.terminated = true // the terminate landed; only its answer is lost
			f.drop(w)
			return
		}
		if f.terminateStatus != 0 {
			w.WriteHeader(f.terminateStatus)
			body := f.terminateBody
			if body == "" {
				body = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidStateSetting</Code><Message><![CDATA[Status can not be set from [Terminated] to [Terminated]]]></Message></Error>`
			}
			_, _ = io.WriteString(w, body)
			return
		}
		f.terminated = true
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchObject</Code><Message><![CDATA[ODPS-0420111: Project not found - 'p'.]]></Message></Error>`)
	}
}

// drop closes the connection without an answer, as a load balancer closing a
// stale keep-alive connection does: the client sees EOF.
func (f *fakeODPS) drop(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	require.True(f.t, ok)
	conn, _, err := hj.Hijack()
	require.NoError(f.t, err)
	_ = conn.Close()
}

func taskResultXML(status, content string) string {
	var b strings.Builder
	b.WriteString(`<Instance><Tasks><Task Type="SQL"><Name>AnonymousSQLTask</Name><Status>` + status + `</Status><Result Transform="" Format="text">`)
	_ = xml.EscapeText(&b, []byte(content))
	b.WriteString(`</Result></Task></Tasks></Instance>`)
	return b.String()
}

type fakeState struct {
	submits          []submission
	polls            int
	terminates       int
	terminated       bool
	pollsAtTerminate int
}

func (f *fakeODPS) snapshot() fakeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeState{submits: append([]submission(nil), f.submits...), polls: f.polls, terminates: f.terminates, terminated: f.terminated, pollsAtTerminate: f.pollsAtTerminate}
}

func (f *fakeODPS) set(fn func(*fakeODPS)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}
