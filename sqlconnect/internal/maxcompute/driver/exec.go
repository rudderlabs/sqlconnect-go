package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// ErrMultipleStatements is returned for text with several statements that the
// caller did not mark as a script.
var ErrMultipleStatements = errors.New("maxcompute: several statements need WithScript")

// ErrRefusedIdentifier is returned for a statement that holds a NUL byte,
// which is how the dialect marks a name it refused to quote.
var ErrRefusedIdentifier = errors.New("maxcompute: the statement holds a refused identifier or a NUL byte")

// bind renders the bind arguments into the statement text: MaxCompute has no
// server-side binds.
func bind(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	values := make([]any, len(args))
	for i, a := range args {
		if a.Name != "" {
			return "", mcerr.Local(mcerr.CategoryRefused, "named bind arguments are not supported; use ?")
		}
		values[i] = a.Value
	}
	text, err := mcsql.Bind(query, values)
	if err != nil {
		return "", mcerr.LocalWrap(mcerr.CategoryRefused, err.Error(), err)
	}
	return text, nil
}

// prepareText checks the statement text and adds the terminator the server needs.
func prepareText(ctx context.Context, query string) (string, error) {
	if strings.IndexByte(query, 0) >= 0 {
		return "", mcerr.LocalWrap(mcerr.CategoryRefused, ErrRefusedIdentifier.Error(), ErrRefusedIdentifier)
	}
	tokens, err := mcsql.Scan(query)
	if err != nil {
		return "", mcerr.LocalWrap(mcerr.CategorySyntax, "the statement cannot be scanned: "+err.Error(), err)
	}
	statements := mcsql.Statements(tokens)
	if len(statements) == 0 {
		return "", mcerr.Local(mcerr.CategorySyntax, "the statement is empty")
	}
	if len(statements) > 1 && !isScript(ctx) {
		return "", mcerr.LocalWrap(mcerr.CategoryRefused, "the text holds several statements; a script needs WithScript", ErrMultipleStatements)
	}
	for _, s := range statements {
		if err := checkSet(s); err != nil {
			return "", mcerr.LocalWrap(mcerr.CategoryRefused, err.Error(), err)
		}
	}
	return mcsql.Terminate(query, tokens), nil
}

// run submits text as one job and waits for it to succeed. kind is "exec" or
// "query"; a script is logged as "script".
func (c *Connector) run(ctx context.Context, text, kind string) (*odps.Instance, error) {
	job := newJobLog(ctx, kind, text)
	start := time.Now()
	inst, err := c.submit(ctx, text)
	submitted := time.Since(start)
	if err != nil {
		// A job that failed synchronously has an instance id on its error.
		if e, ok := errors.AsType[*mcerr.Error](err); ok && e.InstanceID != "" {
			job.submitted(e.InstanceID, submitted)
			job.finished(e.InstanceID, start, err)
			return nil, err
		}
		job.finished("", start, err)
		return nil, err
	}
	job.submitted(inst.Id(), submitted)
	if fn := instanceIDHandler(ctx); fn != nil {
		fn(inst.Id())
	}
	if err := c.wait(ctx, inst); err != nil {
		job.finished(inst.Id(), start, err)
		return nil, err
	}
	job.finished(inst.Id(), start, nil)
	return inst, nil
}

type submitted struct {
	inst *odps.Instance
	err  error
}

// submit creates the job. The SDK call takes no context: when ctx ends first,
// the call is abandoned and the job it may still create is terminated in the
// background.
func (c *Connector) submit(ctx context.Context, text string) (*odps.Instance, error) {
	if err := ctx.Err(); err != nil {
		e := mcerr.Context(err, "", "not submitted")
		e.Stop = string(sqlconnect.StopMissed)
		return nil, e
	}
	opt := options.NewSQLTaskOptions()
	opt.TaskName = TaskName
	opt.Hints = jobHints(ctx, c.cfg.DefaultSchema)
	client := c.cfg.Client
	done := make(chan submitted, 1)
	go func() {
		inst, err := client.ExecSQlWithOption(text, opt)
		done <- submitted{inst, err}
	}()
	select {
	case s := <-done:
		if s.err != nil {
			if s.inst != nil && s.inst.Id() != "" {
				// The SDK returns a job that failed synchronously together
				// with the job's failure text.
				return nil, mcerr.FromJob(s.err.Error()).WithInstance(s.inst.Id())
			}
			return nil, mcerr.FromSubmit(s.err)
		}
		if s.inst == nil || s.inst.Id() == "" {
			return nil, mcerr.Local(mcerr.CategoryUnknown, "the server returned no instance")
		}
		return s.inst, nil
	case <-ctx.Done():
		terminateWait := c.cfg.TerminateWait
		// The SDK's own submit retries HTTP 409 for up to 180 s without exposing
		// the instance id, so a job whose submit finally fails cannot be
		// terminated here; the caller already sees OutcomeUnknown.
		go func() {
			if s := <-done; s.err == nil && s.inst != nil {
				_, _ = stopInstance(context.Background(), s.inst, terminateWait, c.cfg.PollInitial)
			}
		}()
		e := mcerr.Context(ctx.Err(), "", "abandoned during submit; a created job is terminated, best effort")
		e.OutcomeUnknown = true
		e.Stop = string(sqlconnect.StopError)
		return nil, e
	}
}

type jobState int

const (
	jobRunning jobState = iota
	jobSucceeded
	jobFailed
)

// taskState reads a job's state from its task statuses. TERMINATED can mean a
// completed instance, so the task statuses decide.
func taskState(tasks []odps.TaskInInstance) jobState {
	if len(tasks) == 0 {
		return jobRunning
	}
	state := jobSucceeded
	for _, t := range tasks {
		switch t.Status {
		case odps.TaskFailed, odps.TaskCancelled, odps.TaskSuspended:
			return jobFailed
		case odps.TaskSuccess:
		default:
			state = jobRunning
		}
	}
	return state
}

func resultState(results []odps.TaskResult) jobState {
	tasks := make([]odps.TaskInInstance, len(results))
	for i, r := range results {
		tasks[i] = odps.TaskInInstance{Status: r.Status}
	}
	return taskState(tasks)
}

// wait polls the job's task status under ctx until it ends. On cancel or
// deadline it terminates the job and waits for it to stop.
func (c *Connector) wait(ctx context.Context, inst *odps.Instance) error {
	id := inst.Id()
	if inst.IsSync() {
		switch resultState(inst.TaskResults()) {
		case jobSucceeded:
			return nil
		case jobFailed:
			return c.failure(ctx, inst)
		}
	}
	interval := c.cfg.PollInitial
	failures := 0
	for {
		var tasks []odps.TaskInInstance
		err := call(ctx, func() error {
			var err error
			tasks, err = inst.GetTasks()
			return err
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return c.stopped(ctxErr, inst)
		}
		if err != nil {
			if mcerr.IsTransport(err) && failures < c.cfg.PollRetries {
				failures++
				sleep(ctx, interval*time.Duration(failures)) // a cancel is seen by the next poll
				continue
			}
			return mcerr.FromSDK(err).WithInstance(id)
		}
		failures = 0
		switch taskState(tasks) {
		case jobSucceeded:
			return nil
		case jobFailed:
			return c.failure(ctx, inst)
		}
		if !sleep(ctx, interval) {
			return c.stopped(ctx.Err(), inst)
		}
		interval = min(interval+c.cfg.PollInitial, c.cfg.PollMax)
	}
}

// failure returns the classified failure text of a failed job.
func (c *Connector) failure(ctx context.Context, inst *odps.Instance) error {
	var results []odps.TaskResult
	err := call(ctx, func() error {
		var err error
		results, err = inst.GetResult()
		return err
	})
	if err != nil {
		return mcerr.FromSDK(err).WithInstance(inst.Id())
	}
	for _, r := range results {
		if text := r.Content(); text != "" {
			return mcerr.FromJob(text).WithInstance(inst.Id())
		}
	}
	return mcerr.Local(mcerr.CategoryUnknown, "the job failed without a message").WithInstance(inst.Id())
}

// sleep waits d or until ctx ends, and reports whether d passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// stopped terminates the job of a statement whose context ended, and returns
// the statement's error with the stop outcome.
func (c *Connector) stopped(ctxErr error, inst *odps.Instance) error {
	outcome, _ := stopInstance(context.Background(), inst, c.cfg.TerminateWait, c.cfg.PollInitial)
	status := "may still run: the terminate was not confirmed"
	switch outcome {
	case sqlconnect.StopLanded:
		status = "terminated"
	case sqlconnect.StopMissed:
		status = "had already ended (the server refused a terminate of a terminated job)"
	}
	e := mcerr.Context(ctxErr, inst.Id(), status)
	e.Stop = string(outcome)
	return e
}

// stopInstance terminates inst and waits up to wait for it to end.
//   - StopLanded: the terminate was accepted and the job ended.
//   - StopMissed: the job had already ended (HTTP 409 from [Terminated] to
//     [Terminated]) or does not exist.
//   - StopError: the terminate failed, or the job did not end within wait.
func stopInstance(ctx context.Context, inst *odps.Instance, wait, interval time.Duration) (sqlconnect.StopOutcome, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	err := call(ctx, inst.Terminate)
	if err != nil {
		if alreadyEnded(err) {
			return sqlconnect.StopMissed, nil
		}
		return sqlconnect.StopError, mcerr.FromSDK(err).WithInstance(inst.Id())
	}
	for {
		var tasks []odps.TaskInInstance
		err := call(ctx, func() error {
			var err error
			tasks, err = inst.GetTasks()
			return err
		})
		if err == nil {
			if taskState(tasks) != jobRunning {
				return sqlconnect.StopLanded, nil
			}
		} else if ctx.Err() == nil && !mcerr.IsTransport(err) {
			return sqlconnect.StopError, mcerr.FromSDK(err).WithInstance(inst.Id())
		}
		if !sleep(ctx, interval) {
			return sqlconnect.StopError, mcerr.Local(mcerr.CategoryUnknown, "the terminated job did not end in time").WithInstance(inst.Id())
		}
	}
}

// alreadyEnded reports a Terminate answer that means the job is not running:
// HTTP 409 InvalidStateSetting "from [Terminated] to [Terminated]", or HTTP 404
// for an instance that does not exist. Any other 409 is a failed terminate.
func alreadyEnded(err error) bool {
	httpErr, ok := httpError(err)
	if !ok {
		return false
	}
	msg := httpErr.ErrorMessage
	if msg == nil {
		msg = restclient.NewErrorMessage(httpErr.Body)
	}
	switch httpErr.StatusCode {
	case http.StatusConflict:
		return msg != nil && msg.ErrorCode == "InvalidStateSetting" && strings.Contains(msg.Message, "[Terminated] to [Terminated]")
	case http.StatusNotFound:
		return true
	}
	return false
}

func httpError(err error) (restclient.HttpError, bool) {
	if e, ok := errors.AsType[restclient.HttpError](err); ok {
		return e, true
	}
	if e, ok := errors.AsType[*restclient.HttpError](err); ok && e != nil {
		return *e, true
	}
	return restclient.HttpError{}, false
}

// StopInstance terminates the job with instanceID and waits up to wait for it
// to end. A job that already ended, or that does not exist, is StopMissed.
func StopInstance(ctx context.Context, client *odps.Odps, instanceID string, wait time.Duration) (sqlconnect.StopOutcome, error) {
	if instanceID == "" {
		return sqlconnect.StopError, mcerr.Local(mcerr.CategoryRefused, "no instance id")
	}
	if wait <= 0 {
		wait = 30 * time.Second
	}
	return stopInstance(ctx, client.Instance(instanceID), wait, 200*time.Millisecond)
}
