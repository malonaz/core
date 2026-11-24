// What tsunade's callers may do (go/authentication/v2).
local ai(methods) = ['/malonaz.ai.ai_service.v1.AiService/' + method for method in methods];
local grants(roles) = [{ role: role, scope: '*' } for role in roles];

{
  service_accounts: [
    // CLI clients, by API key.
    { id: 'sgpt', grants: grants(['ai:model:reader', 'ai:message', 'ai:chat']) },
    { id: 'mailbot', grants: grants(['ai:chat', 'ai:message', 'ai:speech_to_text', 'ai:text_to_text']) },
    // The service itself: what onyx hands to its Start. It calls nothing.
    { id: 'ai-service' },
  ],
  roles: [
    { id: 'ai:chat', rules: [{ methods: ai(['CreateChat', 'UpdateChat', 'DeleteChat', 'ListChats', 'GetChat']) }] },
    { id: 'ai:message', rules: [{ methods: ai([
      'CreateMessage',
      'UpdateMessage',
      'DeleteMessage',
      'ListMessages',
      'GetMessage',
      'GenerateMessage',
      'StreamGenerateMessage',
    ]) }] },
    { id: 'ai:speech_to_text', rules: [{ methods: ai(['SpeechToText', 'SpeechToTextStream']) }] },
    { id: 'ai:text_to_text', rules: [{ methods: ai(['TextToText', 'TextToTextStream']) }] },
    { id: 'ai:model:reader', rules: [{ methods: ai(['ListModels']) }] },
    // Anyone asks what they could call.
    { id: 'public', rules: [{ methods: ['/malonaz.authentication.v2.AuthorizationService/TestMethods'] }] },
  ],
  public_roles: ['public'],
}
