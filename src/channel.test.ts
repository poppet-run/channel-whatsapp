import { describe, expect, test } from "bun:test";

import { MessagingChannel, type IncomingMessage } from "@poppet/gateway";
import type { WAMessageKey } from "baileys";
import { Effect, Option, Stream } from "effect";

import { whatsappChannelLayer } from "#src/channel";
import type {
  CloseReason,
  Connect,
  Content,
  ConnectionEvents,
  WhatsAppConnection,
  WhatsAppMessage,
} from "#src/socket";

const owner = "447700900100";
const ownerJid = `${owner}@s.whatsapp.net`;
const ownerLid = "11223344@lid";
const groupId = "120363000000000001@g.us";
const botJid = "447700900200:3@s.whatsapp.net";
const botLid = "99887766:3@lid";

type Group = Awaited<ReturnType<WhatsAppConnection["group"]>>;

/** A Baileys stand-in that opens at once and records what the channel sends. */
const fakeSocket = (
  options: {
    close?: CloseReason;
    group?: Group;
    sendFailure?: boolean;
    batches?: ReadonlyArray<ReadonlyArray<WhatsAppMessage>>;
  } = {},
) => {
  const sent: Array<{ jid: string; content: Content; messageId?: string | undefined }> = [];

  const connection: WhatsAppConnection = {
    user: { id: botJid, lid: botLid },
    send: async (jid, content, messageId) => {
      if (options.sendFailure === true) throw new Error("socket closed");
      sent.push({ jid, content, messageId });

      return messageId ?? "REACTION";
    },
    typing: async () => {},
    group: async () =>
      options.group ?? {
        participants: [{ id: "99887766@lid", phoneNumber: "447700900200@s.whatsapp.net" }],
      },
    phoneNumberFor: async (lid) => (lid === ownerLid ? ownerJid : null),
  };

  const connect: Connect = async (events: ConnectionEvents) => {
    if (options.close !== undefined) events.close(options.close);
    else {
      events.open(connection);

      for (const batch of options.batches ?? []) events.messages(batch);
    }

    return () => {};
  };

  return { connect, sent };
};

const run = <A, E>(
  settings: { ownerId: string; chatId: string },
  socket: ReturnType<typeof fakeSocket>,
  use: (channel: MessagingChannel["Service"]) => Effect.Effect<A, E>,
  opens = true,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const channel = yield* MessagingChannel;

      if (opens) yield* channel.accountId;

      return yield* use(channel);
    }).pipe(Effect.provide(whatsappChannelLayer(settings, socket.connect))),
  );

const inGroup = { ownerId: owner, chatId: groupId };

const groupMessage = (
  id: string,
  key: Partial<WAMessageKey>,
  text: string,
  context?: { mentionedJid?: string[]; stanzaId?: string; participant?: string },
): WhatsAppMessage => ({
  key: { remoteJid: groupId, id, fromMe: false, participant: ownerJid, ...key },
  message:
    context === undefined
      ? { conversation: text }
      : { extendedTextMessage: { text, contextInfo: context } },
});

const source: IncomingMessage = {
  messageId: "A1",
  scopeId: groupId,
  parentId: groupId,
  conversationId: groupId,
  senderId: owner,
  text: "Hello",
  mentioned: true,
};

describe("whatsappChannelLayer", () => {
  test("accepts only the owner's text in the group, by number or LID, and detects mentions", async () => {
    const socket = fakeSocket({
      batches: [
        [
          groupMessage("A1", {}, "@447700900200 summarise this", {
            mentionedJid: ["447700900200@s.whatsapp.net"],
          }),
          groupMessage("A2", { participant: "555@s.whatsapp.net" }, "from someone else"),
          groupMessage("A3", { remoteJid: "120363000000000002@g.us" }, "from another group"),
          groupMessage("A4", { fromMe: true }, "from the linked account"),
          groupMessage("A5", { participant: ownerLid }, "@99887766 by LID", {
            mentionedJid: ["99887766@lid"],
          }),
          groupMessage("A6", { participant: ownerLid }, "a reply", {
            stanzaId: "B1",
            participant: "99887766@lid",
          }),
          groupMessage("A7", {}, "not addressed to Poppet"),
        ],
      ],
    });

    const received = await run(inGroup, socket, (channel) =>
      channel.messages.pipe(Stream.take(4), Stream.runCollect),
    );

    expect(received).toEqual([
      { ...source, messageId: "A1", text: "summarise this" },
      { ...source, messageId: "A5", text: "by LID" },
      { ...source, messageId: "A6", text: "a reply" },
      { ...source, messageId: "A7", text: "not addressed to Poppet", mentioned: false },
    ]);
  });

  test("treats every owner message in the private chat as addressed to Poppet", async () => {
    const socket = fakeSocket({
      batches: [
        [
          {
            key: { remoteJid: ownerLid, remoteJidAlt: ownerJid, id: "C1", fromMe: false },
            message: { conversation: "/status" },
          },
          {
            key: { remoteJid: "555@s.whatsapp.net", id: "C2", fromMe: false },
            message: { conversation: "a stranger" },
          },
        ],
      ],
    });

    const received = await run({ ownerId: owner, chatId: owner }, socket, (channel) =>
      channel.messages.pipe(Stream.runHead),
    );

    expect(Option.getOrNull(received)).toEqual({
      messageId: "C1",
      scopeId: owner,
      parentId: owner,
      conversationId: owner,
      senderId: owner,
      text: "/status",
      mentioned: true,
    });
  });

  test("sends with a nonce-derived ID, reacts to the source key, and rejects other chats", async () => {
    const socket = fakeSocket({ batches: [[groupMessage("A1", { participant: ownerLid }, "Hi")]] });

    const result = await run(inGroup, socket, (channel) =>
      Effect.gen(function* () {
        yield* channel.messages.pipe(Stream.runHead);
        yield* channel.mark(source, "working");
        const first = yield* channel.send(groupId, "Reply", "nonce-1");
        const retry = yield* channel.send(groupId, "Reply", "nonce-1");
        const outside = yield* channel.destination("120363000000000002@g.us").pipe(Effect.flip);

        return { first, retry, outside: outside.reason, account: yield* channel.accountId };
      }),
    );

    expect(result.first).toMatch(/^3EB0[0-9A-F]{18}$/);
    expect(result.retry).toBe(result.first);
    expect(result.outside).toBe("permission");
    expect(result.account).toBe("447700900200");

    expect(socket.sent[0]).toEqual({
      jid: groupId,
      content: {
        react: {
          text: "👀",
          key: { remoteJid: groupId, id: "A1", fromMe: false, participant: ownerLid },
        },
      },
      messageId: undefined,
    });

    expect(socket.sent[1]).toEqual({
      jid: groupId,
      content: { text: "Reply" },
      messageId: result.first,
    });
  });

  test("reports a failed send as uncertain", async () => {
    const error = await run(inGroup, fakeSocket({ sendFailure: true }), (channel) =>
      channel.send(groupId, "Reply", "nonce").pipe(Effect.flip),
    );

    expect(error.reason).toBe("uncertain");
  });

  test.each([
    ["the linked account has left", { participants: [] }],
    [
      "only admins can post",
      { announce: true, participants: [{ id: "447700900200@s.whatsapp.net", admin: null }] },
    ],
  ])("refuses access when %s", async (_, group) => {
    const error = await run(inGroup, fakeSocket({ group }), (channel) =>
      channel.checkAccess(source, groupId).pipe(Effect.flip),
    );

    expect(error.reason).toBe("permission");
  });

  test("stays disconnected and asks for a new link when WhatsApp logs the device out", async () => {
    const result = await run(
      inGroup,
      fakeSocket({ close: "unlinked" }),
      (channel) =>
        Effect.gen(function* () {
          yield* Effect.sleep("10 millis");

          return {
            status: yield* channel.status,
            send: (yield* channel.send(groupId, "Reply", "nonce").pipe(Effect.flip)).reason,
          };
        }),
      false,
    );

    expect(result.status).toMatchObject({ connected: false, account: null });
    expect(String(result.status["error"])).toContain("gateway whatsapp link");
    // Nothing reached WhatsApp, so the gateway can retry without risking a duplicate.
    expect(result.send).toBe("unavailable");
  });
});
