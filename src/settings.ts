import { join } from "node:path";

import { channelSettings } from "@poppet/gateway";
import { Schema } from "effect";

/** An international phone number without the plus sign, as WhatsApp uses in IDs. */
export const PhoneNumber = Schema.String.check(Schema.isPattern(/^[1-9][0-9]{6,14}$/));

/** The owner's phone number for the private chat, or a group JID. */
const ChatId = Schema.String.check(
  Schema.isPattern(/^(?:[1-9][0-9]{6,14}|[0-9]{1,20}(?:-[0-9]{1,20})?@g\.us)$/),
);

/** The linked device's keys live apart from these settings, in the auth directory. */
export const WhatsAppSettings = Schema.Struct({
  ownerId: PhoneNumber,
  chatId: ChatId,
}).check(
  Schema.makeFilter(
    // A private chat is identified by its other member, so only the owner's own chat qualifies.
    (settings) => settings.chatId.endsWith("@g.us") || settings.chatId === settings.ownerId,
  ),
);

export type WhatsAppSettings = typeof WhatsAppSettings.Type;

export const whatsappSettings = channelSettings("whatsapp", "WhatsApp", WhatsAppSettings);

/** Baileys stores the linked device's credentials and Signal keys as many small files. */
export const authDirectory = (dataDirectory: string) =>
  join(dataDirectory, "channels", "whatsapp-auth");
