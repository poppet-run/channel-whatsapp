import { chmod, mkdir, rm } from "node:fs/promises";

import type {
  GroupMetadata,
  SocketConfig,
  UserFacingSocketConfig,
  WAMessageContent,
  WAMessageKey,
  WASocket,
} from "baileys";
import { Option, Schema } from "effect";

/** A received message with wrappers such as disappearing-message envelopes removed. */
export interface WhatsAppMessage {
  readonly key: WAMessageKey;
  readonly message: WAMessageContent | undefined;
}

export type Content =
  | { readonly text: string }
  | { readonly react: { readonly text: string; readonly key: WAMessageKey } };

/** The parts of an open Baileys socket that the channel uses. */
export interface WhatsAppConnection {
  readonly user: { readonly id: string; readonly lid?: string | undefined };
  /** Returns the sent message's ID, or null when Baileys reports none. */
  readonly send: (jid: string, content: Content, messageId?: string) => Promise<string | null>;
  readonly typing: (jid: string) => Promise<void>;
  readonly group: (jid: string) => Promise<Pick<GroupMetadata, "participants" | "announce">>;
  readonly phoneNumberFor: (lid: string) => Promise<string | null>;
}

/** Unlinked needs a new link; replaced means another client took over these credentials. */
export type CloseReason = "unlinked" | "replaced" | "lost";

export interface ConnectionEvents {
  readonly open: (connection: WhatsAppConnection) => void;
  readonly messages: (messages: ReadonlyArray<WhatsAppMessage>) => void;
  readonly close: (reason: CloseReason) => void;
}

/** Starts one connection attempt and returns a function that ends it. */
export type Connect = (events: ConnectionEvents) => Promise<() => void>;

const silent: SocketConfig["logger"] = {
  level: "silent",
  child: () => silent,
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

// Baileys reports disconnects as Boom errors that carry an HTTP-like status code.
const Disconnect = Schema.Struct({ output: Schema.Struct({ statusCode: Schema.Number }) });

const statusCode = (error: Error | undefined) =>
  Option.getOrUndefined(Schema.decodeUnknownOption(Disconnect)(error))?.output.statusCode;

const closeReason = (error: Error | undefined): CloseReason => {
  const code = statusCode(error);

  // 401 is a logout from the phone; 403 is an account WhatsApp refuses.
  if (code === 401 || code === 403) return "unlinked";

  return code === 440 ? "replaced" : "lost";
};

// The version Baileys ships with goes stale, and WhatsApp refuses clients that are too old.
let latestVersion: Promise<[number, number, number] | null> | undefined;

const prepare = async (directory: string) => {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const baileys = await import("baileys");
  const auth = await baileys.useMultiFileAuthState(directory);

  latestVersion ??= baileys.fetchLatestBaileysVersion().then(
    (latest) => (latest.isLatest ? latest.version : null),
    () => null,
  );

  const version = await latestVersion;

  const config: UserFacingSocketConfig = {
    auth: auth.state,
    logger: silent,
    browser: baileys.Browsers.macOS("Poppet"),
    markOnlineOnConnect: false,
    shouldSyncHistoryMessage: () => false,
  };

  if (version !== null) config.version = version;

  const open = () => {
    const socket = baileys.makeWASocket(config);

    socket.ev.on("creds.update", auth.saveCreds);

    return socket;
  };

  return { baileys, auth, open };
};

const connection = (socket: WASocket, user: NonNullable<WASocket["user"]>): WhatsAppConnection => ({
  user: { id: user.id, lid: user.lid },
  send: async (jid, content, messageId) =>
    (await socket.sendMessage(jid, content, messageId === undefined ? {} : { messageId }))?.key
      .id ?? null,
  typing: (jid) => socket.sendPresenceUpdate("composing", jid),
  group: (jid) => socket.groupMetadata(jid),
  phoneNumberFor: (lid) => socket.signalRepository.lidMapping.getPNForLID(lid),
});

/** Connects with the linked device's saved credentials. */
export const baileysConnect =
  (directory: string): Connect =>
  async (events) => {
    const { baileys, auth, open } = await prepare(directory);

    if (!auth.state.creds.registered) {
      events.close("unlinked");

      return () => {};
    }

    const socket = open();

    socket.ev.on("connection.update", (update) => {
      if (update.connection === "open" && socket.user !== undefined)
        events.open(connection(socket, socket.user));
      else if (update.connection === "close")
        events.close(closeReason(update.lastDisconnect?.error));
    });

    // Messages received while offline arrive as append; history sync uses another event.
    socket.ev.on("messages.upsert", ({ messages }) => {
      events.messages(
        messages.map((message) => ({
          key: message.key,
          message: baileys.normalizeMessageContent(message.message),
        })),
      );
    });

    return () => void socket.end(undefined);
  };

/** Resolves true once open, or false when WhatsApp asks for a restart after pairing. */
const untilOpen = (socket: WASocket, signal: AbortSignal, onQr = async () => {}) =>
  new Promise<boolean>((resolve, reject) => {
    signal.throwIfAborted();
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });

    socket.ev.on("connection.update", (update) => {
      if (update.qr !== undefined) onQr().catch(reject);

      if (update.connection === "open") resolve(true);
      else if (update.connection === "close")
        if (statusCode(update.lastDisconnect?.error) === 515) resolve(false);
        else reject(new Error("WhatsApp closed the connection."));
    });
  });

/** Links a new device to the number's account, replacing any earlier link; returns the user JID. */
export const linkDevice = async (
  directory: string,
  phoneNumber: string,
  showCode: (code: string) => void,
  signal: AbortSignal,
) => {
  await rm(directory, { recursive: true, force: true });
  const { auth, open } = await prepare(directory);
  let requested = false;

  // WhatsApp restarts the stream once pairing succeeds, so linking takes a second socket.
  /* eslint-disable no-await-in-loop -- Each socket must close before the next one opens. */
  for (let attempt = 0; attempt < 3; attempt++) {
    const socket = open();

    try {
      const opened = await untilOpen(socket, signal, async () => {
        if (requested || auth.state.creds.registered) return;
        requested = true;
        showCode(await socket.requestPairingCode(phoneNumber));
      });

      if (opened && socket.user !== undefined) {
        await auth.saveCreds();

        return socket.user.id;
      }
    } finally {
      void socket.end(undefined);
    }
  }

  /* eslint-enable no-await-in-loop */
  throw new Error("WhatsApp did not finish linking.");
};

/** Lists the groups the linked account belongs to, or null without a linked device. */
export const listGroups = async (directory: string, signal: AbortSignal) => {
  const { auth, open } = await prepare(directory);

  if (!auth.state.creds.registered) return null;
  const socket = open();

  try {
    if (!(await untilOpen(socket, signal))) throw new Error("WhatsApp asked for a restart.");

    return Object.values(await socket.groupFetchAllParticipating()).map((group) => ({
      id: group.id,
      subject: group.subject,
    }));
  } finally {
    void socket.end(undefined);
  }
};
