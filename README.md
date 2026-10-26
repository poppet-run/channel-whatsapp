# Poppet WhatsApp channel

An external [Poppet](../poppet) messaging channel for WhatsApp. It connects as a
linked device through [Baileys](https://github.com/WhiskeySockets/Baileys), the
WhatsApp Web protocol, so it needs no Meta business account and no public
webhook.

Baileys is unofficial. Automating a WhatsApp account is against WhatsApp's
terms, and WhatsApp may ban the number. Use a spare number, not your own.

## Chats and conversations

The channel serves one configured chat and one owner. Only the owner can invoke
Poppet; everyone in the chat can read its replies.

| Chat                                     | Conversation                                                                |
| ---------------------------------------- | --------------------------------------------------------------------------- |
| Your private chat with the linked number | The whole chat is one conversation. Every message is addressed to Poppet.   |
| Group                                    | The whole chat is one conversation. Mention Poppet or reply to it to start. |

WhatsApp has no threads, so the conversation ID is the chat ID: the owner's
phone number for the private chat, or the group's JID, such as
`120363012345678901@g.us`. It is also the home conversation.

Mentions of the linked number are removed before the gateway sees the text, so
`/status`, `/cancel`, `/new`, `/approve`, and `/deny` work as they do in
Discord. Poppet reacts with 👀 while working and 👍 when finished.

## Install

The package has peer dependencies on Poppet's `@poppet/gateway` and Effect
4.0.4, which are not published. For now, install Baileys and link this checkout
into Poppet's gateway directory next to a Poppet checkout at `../poppet`:

```fish
bun install
set data (set -q POPPET_DATA_DIR; and echo $POPPET_DATA_DIR; or echo ~/.config/poppet)
mkdir -p $data/gateways/node_modules/@poppet
ln -s (pwd) $data/gateways/node_modules/@poppet/channel-whatsapp
```

Then list the entry point in `<POPPET_DATA_DIR>/config.json`:

```json
{
  "gatewayPackages": ["@poppet/channel-whatsapp/registration"]
}
```

## Setup

Phone numbers are in international format with digits only, such as
`447700900123`. Stop the gateway worker first: the commands below connect with
the same linked device, and WhatsApp allows one connection per device.

1. Link the spare number. The command prints an eight-character code. On that
   phone, open **Settings → Linked devices → Link a device**, choose **Link with
   phone number instead**, and enter the code within three minutes. Linking
   again replaces the previous link.

   ```fish
   poppet gateway whatsapp link <linked-phone-number>
   ```

2. For a group, add the linked number to it, then find the group's JID:

   ```fish
   poppet gateway whatsapp groups
   ```

3. Save the owner and chat, and start the worker:

   ```fish
   poppet gateway whatsapp setup <owner-phone-number> <owner-phone-number-or-group-jid>
   poppet gateway work
   ```

The owner and chat are stored in `<POPPET_DATA_DIR>/channels/whatsapp.json` with
`0600` permissions. The linked device's keys are stored in
`<POPPET_DATA_DIR>/channels/whatsapp-auth/`, a `0700` directory. Restart the
worker after changing either.

## Limits

- Replies are sent as plain text, so Markdown from the model is shown as written.
- Each reply chunk gets a message ID derived from the gateway's nonce, so a retry
  reuses it. If a send fails without a clear answer, the gateway records it as
  uncertain, and a retry may still duplicate it.
- Messages that arrive while the worker is stopped are delivered when it
  reconnects. The gateway ignores messages it has already accepted.
- Reactions on messages received before a worker restart use a rebuilt message
  key and may not appear.
- If WhatsApp logs the device out, the channel stays disconnected until you link
  again and restart the worker. `poppet gateway status` shows the reason.
- Only text messages are read.

## Development

The package resolves `@poppet/gateway` and `effect` from the sibling Poppet
checkout through `tsconfig.json` paths. Only Baileys is installed here.

```fish
bun install
bun check
bun test
```
