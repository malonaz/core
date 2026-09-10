// The sat fixtures: the in-process processor as a target, one queue per
// processor method so each has its own policy. Timings mirror the constants
// in sat_test.go, which the tests derive their expectations from.
local processor = 'targets/test-processor';
local method(name) = '/malonaz.test.scheduler.processor.v1.Processor/' + name;
local handler(name) = { method: method(name), target: processor };
local queue(id, name, policy) = {
  queue_id: id,
  queue: { policy: policy, handlers: [handler(name)] },
};

{
  targets: [
    {
      target_id: 'test-processor',
      target: { url: 'http://localhost:9091', headers: { 'x-test-header': 'hello' } },
    },
  ],
  queues: [
    queue('echo', 'Echo', { attempt_timeout: '5s', max_attempts: 1 }),
    queue('flaky', 'Flaky', {
      attempt_timeout: '5s',
      max_attempts: 3,
      retry_backoff: { initial: '0.3s', max: '5s', multiplier: 2 },
    }),
    queue('sleep', 'Sleep', { attempt_timeout: '10s', max_attempts: 1 }),
    queue('deadline', 'Deadline', {
      attempt_timeout: '1s',
      max_attempts: 2,
      retry_backoff: { initial: '0.2s', max: '1s', multiplier: 1 },
      max_concurrency: 2,
    }),
    queue('progress', 'Progress', { attempt_timeout: '5s', max_attempts: 1 }),
    queue('limited', 'Sleep', { attempt_timeout: '10s', max_attempts: 1, max_concurrency: 2 }),
    // Serialised so the order jobs are claimed in is observable as the order they run in.
    queue('serial', 'Echo', { attempt_timeout: '5s', max_attempts: 1, max_concurrency: 1 }),
    queue('operate', 'Operate', {
      attempt_timeout: '5s',
      max_attempts: 3,
      retry_backoff: { initial: '0.1s', max: '1s', multiplier: 1 },
      // Proves an unfinished operation is not retried even when its code is retryable.
      retryable_codes: ['FAILED_PRECONDITION', 'INTERNAL'],
    }),
    // Only the bootstrap tests use this one: it is paused across a bootstrap.
    queue('bootstrap-paused', 'Echo', { attempt_timeout: '5s', max_attempts: 1 }),
  ],
}
