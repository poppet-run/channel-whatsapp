import { ChannelEnvironment, ChannelError, type ChannelRegistration } from "@poppet/gateway";
import { Console, Effect, Schema } from "effect";

import { PhoneNumber, WhatsAppSettings, authDirectory, whatsappSettings } from "#src/settings";

const usage =
  "Use gateway whatsapp link <phone-number>, gateway whatsapp groups, or gateway whatsapp setup <owner-phone-number> <chat-id>.";

const configuration = (message: string) => new ChannelError({ reason: "configuration", message });

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
  setupHelp: "link <phone-number> | groups | setup <owner-phone-number> <chat-id>",
  setup: (args) =>
    Effect.gen(function* () {
      const { dataDirectory } = yield* ChannelEnvironment;

      if (args[0] === "link" && args.length === 2) return yield* link(dataDirectory, args[1]!);

      if (args[0] === "groups" && args.length === 1) return yield* groups(dataDirectory);

      if (args[0] === "setup" && args.length === 3)
        return yield* setup(dataDirectory, args[1]!, args[2]!);

      return yield* Effect.fail(configuration(usage));
    }),
  load: whatsappSettings.loader((settings, dataDirectory) =>
    Promise.all([import("#src/channel"), import("#src/socket")]).then(
      ([{ whatsappChannelLayer }, { baileysConnect }]) =>
        whatsappChannelLayer(settings, baileysConnect(authDirectory(dataDirectory))),
    ),
  ),
};

export default whatsapp;
