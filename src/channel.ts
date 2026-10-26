import { createHash } from "node:crypto";

import { ChannelError, MessagingChannel, type IncomingMessage } from "@poppet/gateway";
import type { WAMessageKey } from "baileys";
import { Deferred, Effect, Layer, Queue, Stream } from "effect";

import type { WhatsAppSettings } from "#src/settings";
import type { CloseReason, Connect, WhatsAppConnection, WhatsAppMessage } from "#src/socket";

/** Drops the device suffix: `447700900123:12@s.whatsapp.net` becomes `447700900123@s.whatsapp.net`. */
const normalize = (jid: string | null | undefined) => {
  const at = jid?.indexOf("@") ?? -1;

  if (jid === null || jid === undefined || at < 1) return null;

  return `${jid.slice(0, at).split(":")[0]}${jid.slice(at)}`;
};

const ownJids = (current: WhatsAppConnection) =>
  [normalize(current.user.id), normalize(current.user.lid)].filter((jid) => jid !== null);

const failureMessages = {
  unlinked:
    "No linked WhatsApp device, or WhatsApp logged it out or refused the account. Run gateway whatsapp link.",
  replaced: "Another client is using this linked device. Stop it or link Poppet again.",
  lost: "WhatsApp connection lost; reconnecting.",
} as const satisfies Record<CloseReason, string>;

const whatsappFailure = (cause: unknown, sending = false) =>
  cause instanceof ChannelError
    ? cause
    : new ChannelError({
        reason: sending ? "uncertain" : "unavailable",
        message: "WhatsApp operation failed. Check the linked device's connection.",
      });

// Baileys IDs are 3EB0 and 18 hex digits; deriving one from the nonce keeps it stable across retries.
const messageId = (nonce: string) =>
  `3EB0${createHash("sha256").update(nonce).digest("hex").slice(0, 18).toUpperCase()}`;

const reactions = { working: "👀", completed: "👍" } as const;

export const whatsappChannelLayer = (settings: WhatsAppSettings, connect: Connect) =>
  Layer.effect(
    MessagingChannel,
    Effect.gen(function* () {
      const messages = yield* Queue.make<IncomingMessage>({ capacity: 64, strategy: "dropping" });

      // Owner checks may look up a LID, so received messages are handled in order on one fiber.
      const received = yield* Queue.make<readonly [WhatsAppConnection, WhatsAppMessage]>({
        capacity: 256,
        strategy: "dropping",
      });

      // Reactions need the source message's full key, which the gateway does not keep.
      const sourceKeys = new Map<string, WAMessageKey>();
      const identity = yield* Deferred.make<string>();
      const group = settings.chatId.endsWith("@g.us");
      const ownerJid = `${settings.ownerId}@s.whatsapp.net`;
      const chatJid = group ? settings.chatId : ownerJid;
      let connection: WhatsAppConnection | null = null;
      let connectionError: string | null = null;
      let dropped = 0;

      const requireReady = () => {
        if (connection === null)
          throw new ChannelError({ reason: "unavailable", message: "WhatsApp is disconnected." });

        return connection;
      };

      const target = (conversationId: string) => {
        if (conversationId !== settings.chatId)
          throw new ChannelError({
            reason: "permission",
            message: "Choose the configured WhatsApp chat.",
          });
      };

      // WhatsApp may address the owner by phone number or by LID, with the other as an alternate.
      const isOwner = async (
        current: WhatsAppConnection,
        jids: ReadonlyArray<string | null | undefined>,
      ) => {
        // Look up a LID only when no earlier ID matched.
        for (const jid of jids.map(normalize)) {
          if (jid === ownerJid) return true;

          // eslint-disable-next-line no-await-in-loop
          if (jid?.endsWith("@lid") && normalize(await current.phoneNumberFor(jid)) === ownerJid)
            return true;
        }

        return false;
      };

      const incoming = async (current: WhatsAppConnection, { key, message }: WhatsAppMessage) => {
        const text = message?.conversation ?? message?.extendedTextMessage?.text;

        if (key.fromMe === true || !key.id || !text) return;

        if (group) {
          if (key.remoteJid !== settings.chatId) return;

          if (!(await isOwner(current, [key.participant, key.participantAlt]))) return;
        } else if (!(await isOwner(current, [key.remoteJid, key.remoteJidAlt]))) return;

        const context = message?.extendedTextMessage?.contextInfo;
        const own = ownJids(current);

        const mentioned =
          !group ||
          (context?.mentionedJid ?? []).some((jid) => own.includes(normalize(jid) ?? "")) ||
          (context?.stanzaId != null && own.includes(normalize(context.participant) ?? ""));

        // Mentions appear in the text as @ and the mentioned number or LID.
        const stripped = own
          .reduce(
            (result, jid) =>
              result.replaceAll(new RegExp(`@${jid.slice(0, jid.indexOf("@"))}(?![0-9])`, "g"), ""),
            text,
          )
          .trim();

        if (!stripped || stripped.length > 8192) return;

        sourceKeys.set(key.id, key);

        if (sourceKeys.size > 256) sourceKeys.delete(sourceKeys.keys().next().value!);

        if (
          !Queue.offerUnsafe(messages, {
            messageId: key.id,
            scopeId: settings.chatId,
            parentId: settings.chatId,
            conversationId: settings.chatId,
            senderId: settings.ownerId,
            text: stripped,
            mentioned,
          })
        )
          dropped++;
      };

      yield* Stream.fromQueue(received).pipe(
        Stream.runForEach(([current, message]) =>
          Effect.promise(() => incoming(current, message)).pipe(
            Effect.catchDefect(() => Effect.void),
          ),
        ),
        Effect.forkScoped,
      );

      // One connection lives until it closes; the loop then waits before connecting again.
      const session = Effect.callback<never, CloseReason>((resume) => {
        let closed = false;
        let opened: WhatsAppConnection | null = null;

        const ended = connect({
          open: (current) => {
            opened = current;
            connection = current;
            connectionError = null;
            Deferred.doneUnsafe(
              identity,
              Effect.succeed(normalize(current.user.id)!.split("@")[0]!),
            );
          },
          messages: (batch) => {
            for (const message of batch)
              if (opened === null || !Queue.offerUnsafe(received, [opened, message])) dropped++;
          },
          close: (reason) => {
            if (closed) return;
            closed = true;
            connection = null;
            resume(Effect.fail(reason));
          },
        });

        ended.catch(() => {
          if (!closed) {
            closed = true;
            resume(Effect.fail("lost"));
          }
        });

        return Effect.promise(() =>
          ended.then(
            (end) => end(),
            () => {},
          ),
        );
      });

      yield* session.pipe(
        Effect.catch((reason) =>
          Effect.gen(function* () {
            connectionError = failureMessages[reason];

            // Without a link, retrying cannot help until a new link and a worker restart.
            if (reason === "unlinked") return yield* Effect.never;
            yield* Effect.sleep(reason === "replaced" ? "1 minute" : "5 seconds");
          }),
        ),
        Effect.forever,
        Effect.forkScoped,
      );

      const request = <A>(
        operation: (current: WhatsAppConnection) => Promise<A>,
        sending = false,
      ) =>
        Effect.try({ try: requireReady, catch: (cause) => whatsappFailure(cause) }).pipe(
          Effect.flatMap((current) =>
            Effect.tryPromise({
              try: () => operation(current),
              catch: (cause) => whatsappFailure(cause, sending),
            }),
          ),
        );

      // After a restart, rebuild the key from the configured chat and owner.
      const fallbackKey = (id: string) => {
        const key: WAMessageKey = { remoteJid: chatJid, id, fromMe: false };

        if (group) key.participant = ownerJid;

        return key;
      };

      const authorize = (message: IncomingMessage) => {
        if (
          message.scopeId !== settings.chatId ||
          message.parentId !== settings.chatId ||
          message.senderId !== settings.ownerId
        )
          throw new ChannelError({
            reason: "permission",
            message: "This WhatsApp message is outside the configured chat or owner.",
          });
      };

      // In a group, the linked account must still be a member, and an admin if only admins post.
      const checkChat = group
        ? request((current) => current.group(settings.chatId)).pipe(
            Effect.flatMap((metadata) => {
              const own = ownJids(requireReady());

              const member = metadata.participants.find((participant) =>
                [participant.id, participant.lid, participant.phoneNumber].some((jid) =>
                  own.includes(normalize(jid) ?? ""),
                ),
              );

              return member === undefined || (metadata.announce === true && !member.admin)
                ? Effect.fail(
                    new ChannelError({
                      reason: "permission",
                      message: "The linked account cannot post in this WhatsApp group.",
                    }),
                  )
                : Effect.void;
            }),
          )
        : Effect.void;

      const checkAccess = (message: IncomingMessage, conversationId: string) =>
        Effect.try({
          try: () => {
            authorize(message);
            target(conversationId);
          },
          catch: (cause) => whatsappFailure(cause),
        }).pipe(Effect.andThen(checkChat));

      const conversation = (message: IncomingMessage) =>
        Effect.try({
          try: () => authorize(message),
          catch: (cause) => whatsappFailure(cause),
        }).pipe(Effect.as(message.conversationId ?? settings.chatId));

      return {
        id: "whatsapp",
        accountId: Deferred.await(identity),
        policy: {
          ownerId: settings.ownerId,
          scopeId: settings.chatId,
        },
        homeConversationId: settings.chatId,
        messages: Stream.fromQueue(messages),
        textLimit: 4096,
        status: Effect.sync(() => ({
          adapter: "whatsapp",
          account: connection === null ? null : normalize(connection.user.id),
          connected: connection !== null,
          error: connectionError,
          dropped,
        })),
        checkAccess,
        destination: (conversationId) =>
          Effect.try({
            try: () => target(conversationId),
            catch: (cause) => whatsappFailure(cause),
          }).pipe(Effect.andThen(checkChat), Effect.as(settings.chatId)),
        mark: (message, state) =>
          Effect.try({
            try: () => authorize(message),
            catch: (cause) => whatsappFailure(cause),
          }).pipe(
            Effect.andThen(
              request((current) =>
                current.send(chatJid, {
                  react: {
                    text: state === "stopped" ? "" : reactions[state],
                    key: sourceKeys.get(message.messageId) ?? fallbackKey(message.messageId),
                  },
                }),
              ),
            ),
            Effect.asVoid,
          ),
        typing: (conversationId) =>
          Effect.try({
            try: () => target(conversationId),
            catch: (cause) => whatsappFailure(cause),
          }).pipe(Effect.andThen(request((current) => current.typing(chatJid)))),
        conversation,
        // WhatsApp has no threads, so the chat is the only conversation and Poppet never names it.
        naming: {
          sanitize: (title) => title.trim(),
          conversation: (message) =>
            conversation(message).pipe(Effect.map((id) => ({ id, managedName: null }))),
          changes: Stream.empty,
          current: () =>
            Effect.fail(
              new ChannelError({
                reason: "unavailable",
                message: "Poppet does not name WhatsApp chats.",
              }),
            ),
          rename: (message, id) => checkAccess(message, id).pipe(Effect.as(null)),
        },
        send: (conversationId, text, nonce) =>
          Effect.try({
            try: () => target(conversationId),
            catch: (cause) => whatsappFailure(cause),
          }).pipe(
            Effect.andThen(
              request((current) => current.send(chatJid, { text }, messageId(nonce)), true),
            ),
            Effect.flatMap((id) =>
              id === null ? Effect.fail(whatsappFailure(null, true)) : Effect.succeed(id),
            ),
          ),
      } satisfies MessagingChannel["Service"];
    }),
  );
