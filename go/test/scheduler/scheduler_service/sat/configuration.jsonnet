local processor = 'test-processor';
local service = '/malonaz.test.scheduler.processor.v1.Processor/';
local request(name) = 'type.googleapis.com/malonaz.test.scheduler.processor.v1.' + name + 'Request';
local response(name) = 'type.googleapis.com/malonaz.test.scheduler.processor.v1.' + name + 'Response';

{
  processors: [
    {
      id: processor,
      url: 'http://localhost:9091',
      headers: { 'x-test-header': 'hello' },
    },
  ],
  job_type_configurations: [
    {
      job_type_url: request('Echo'),
      processor_id: processor,
      method: service + 'Echo',
      timeout: '5s',
      response_type_url: response('Echo'),
      max_attempts: 1,
    },
    {
      job_type_url: request('Flaky'),
      processor_id: processor,
      method: service + 'Flaky',
      timeout: '5s',
      response_type_url: response('Flaky'),
      max_attempts: 3,
      retry_backoff: { initial: '0.3s', max: '5s', multiplier: 2 },
    },
    {
      job_type_url: request('Sleep'),
      processor_id: processor,
      method: service + 'Sleep',
      timeout: '10s',
      max_attempts: 1,
    },
    {
      job_type_url: request('Deadline'),
      processor_id: processor,
      method: service + 'Deadline',
      timeout: '1s',
      max_attempts: 2,
      retry_backoff: { initial: '0.2s', max: '1s', multiplier: 1 },
    },
    {
      job_type_url: request('Progress'),
      processor_id: processor,
      method: service + 'Progress',
      timeout: '5s',
      max_attempts: 1,
    },
    {
      job_type_url: request('Ignored'),
      processor_id: processor,
      method: service + 'Ignored',
      timeout: '5s',
      max_attempts: 1,
    },
  ],
}
