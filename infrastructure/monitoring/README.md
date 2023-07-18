## Setting up alerting Grafana <> Telegram
# Setup a telegram bot
Send /newbot to @botfather. You will get the bot_token from the response.

# Create telegram group
First thing you need is to create new Chat Group in Telegram App, for example: “Super Dooper Alert Group” with people who need to be alerted.
Invite your bot to this group
Use cURL or just place this on any Browsers Address Bar: https://api.telegram.org/bot<TOKEN>/getUpdates
Previous step should return JSON object, you need to find key “chat” like this one:
"chat":{"id":-456343576,"title":"Super Dooper Alert Group","type":"group","all_members_are_administrators":true}

# Connect it all together.
Go to the UI on grafana and setup a point of contact.
You need that number: -456343576 - just place it in according field after Bot API Token in Telegram Channel configuration page - and thats all!!!

