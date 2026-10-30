import { parseArgs } from "node:util";

import { ChannelEnvironment, ChannelError, type ChannelRegistration } from "@poppet/gateway";
import { Console, Effect, Schema } from "effect";

import { PhoneNumber, WhatsAppSettings, authDirectory, whatsappSettings } from "#src/settings";

const setupHelp =
  "link --phone-number <phone-number> | groups | setup --owner-id <owner-phone-number> --chat-id <chat-id>";

const usage = `Use gateway whatsapp ${setupHelp}.`;

const configuration = (message: string) => new ChannelError({ reason: "configuration", message });

const parseCommand = (args: ReadonlyArray<string>) =>
  Effect.try({
    try: () => {
      const command = args[0];
      if (command !== "link" && command !== "groups" && command !== "setup")
        throw new Error("Expected link, groups, or setup");

      const required =
        command === "link" ? ["phone-number"] : command === "setup" ? ["owner-id", "chat-id"] : [];
      const { values, tokens } = parseArgs({
        args: args.slice(1),
        options: Object.fromEntries(required.map((name) => [name, { type: "string" as const }])),
        strict: true,
        allowPositionals: false,
        tokens: true,
      });

      const names = tokens.filter((token) => token.kind === "option").map((token) => token.name);
      if (names.length !== required.length || new Set(names).size !== required.length)
        throw new Error("Expected each flag exactly once");

      return { command, values };
    },
    catch: () => configuration(usage),
  });

const link = (dataDirectory: string, phoneNumber: string) =>
  Effect.gen(function* () {
    if (!Schema.is(PhoneNumber)(phoneNumber))
      return yield* Effect.fail(
        configuration("Enter the phone number in international format, digits only."),
      );

    // The code arrives while the link is pending, so it is printed before the effect completes.
    const linked = yield* Effect.tryPromise({
      try: (signal) =>
        import("#src/socket").then(({ linkDevice }) =>
          linkDevice(
            authDirectory(dataDirectory),
            phoneNumber,
            (code) =>
              Effect.runSync(
                Console.log(
                  `On the phone for +${phoneNumber}, open Linked devices, choose Link with phone number instead, and enter ${code.slice(0, 4)}-${code.slice(4)}.`,
                ),
              ),
            AbortSignal.any([signal, AbortSignal.timeout(180000)]),
          ),
        ),
      catch: () =>
        new ChannelError({
          reason: "unavailable",
          message:
            "WhatsApp linking failed or timed out. Stop the gateway worker, check the phone number, and enter the code within three minutes.",
        }),
    });

    return `Linked as +${linked.split(/[:@]/)[0]}. Restart the worker to apply it.`;
  });

const groups = (dataDirectory: string) =>
  Effect.tryPromise({
    try: (signal) =>
      import("#src/socket").then(({ listGroups }) =>
        listGroups(
          authDirectory(dataDirectory),
          AbortSignal.any([signal, AbortSignal.timeout(60000)]),
        ),
      ),
    catch: () =>
      new ChannelError({
        reason: "unavailable",
        message: "Cannot list WhatsApp groups. Stop the gateway worker and try again.",
      }),
  }).pipe(
    Effect.flatMap((list) =>
      list === null
        ? Effect.fail(configuration("No linked device. Run gateway whatsapp link first."))
        : Effect.succeed(list),
    ),
    Effect.map((list) =>
      list.length === 0
        ? "The linked account is not in any groups."
        : list.map((group) => `${group.id}  ${group.subject}`).join("\n"),
    ),
  );

const setup = (dataDirectory: string, ownerId: string, chatId: string) =>
  Effect.gen(function* () {
    const settings = yield* Schema.decodeUnknownEffect(WhatsAppSettings)({ ownerId, chatId }).pipe(
      Effect.mapError(() =>
        configuration(
          "Use international phone numbers without +, and either the owner's number as the chat or a group JID ending in @g.us.",
        ),
      ),
    );

    yield* whatsappSettings.save(dataDirectory, settings);

    return "WhatsApp settings saved. Restart the worker to apply them. Only the configured owner can invoke Poppet; chat members can read its replies.";
  });

export const whatsapp: ChannelRegistration = {
  id: "whatsapp",
  name: "WhatsApp",
  setupHelp,
  setup: (args) =>
    Effect.gen(function* () {
      const { command, values } = yield* parseCommand(args);
      const { dataDirectory } = yield* ChannelEnvironment;

      if (command === "link") return yield* link(dataDirectory, values["phone-number"]!);

      if (command === "groups") return yield* groups(dataDirectory);

      return yield* setup(dataDirectory, values["owner-id"]!, values["chat-id"]!);
    }),
  load: whatsappSettings.loader((settings, dataDirectory) =>
    Promise.all([import("#src/channel"), import("#src/socket")]).then(
      ([{ whatsappChannelLayer }, { baileysConnect }]) =>
        whatsappChannelLayer(settings, baileysConnect(authDirectory(dataDirectory))),
    ),
  ),
};

export default whatsapp;
