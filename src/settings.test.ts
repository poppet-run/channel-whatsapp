import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ChannelEnvironment, ChannelInput } from "@poppet/gateway";
import { Effect, Redacted } from "effect";

import { whatsapp } from "#src/registration";
import { whatsappSettings } from "#src/settings";

const setup = (directory: string, args: ReadonlyArray<string>) =>
  Effect.runPromise(
    whatsapp
      .setup(args)
      .pipe(
        Effect.provideService(ChannelEnvironment, { dataDirectory: directory }),
        Effect.provideService(ChannelInput, { readSecret: Effect.succeed(Redacted.make("")) }),
      ),
  );

const withDirectory = async (use: (directory: string) => Promise<void>) => {
  const directory = await mkdtemp(join(tmpdir(), "poppet-whatsapp-"));

  try {
    await use(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
};

describe.concurrent("settings", () => {
  test("setup saves the owner and group privately", () =>
    withDirectory(async (directory) => {
      expect(await Effect.runPromise(whatsappSettings.load(directory))).toBeNull();
      await setup(directory, [
        "setup",
        "--owner-id",
        "447700900100",
        "--chat-id",
        "120363000000000001@g.us",
      ]);
      const path = join(directory, "channels/whatsapp.json");
      expect((await stat(path)).mode & 0o777).toBe(0o600);

      expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
        ownerId: "447700900100",
        chatId: "120363000000000001@g.us",
      });
    }));

  test("setup accepts the owner's private chat", () =>
    withDirectory(async (directory) => {
      await setup(directory, ["setup", "--owner-id", "447700900100", "--chat-id", "447700900100"]);
      const loaded = await Effect.runPromise(whatsappSettings.load(directory));
      expect(loaded?.chatId).toBe("447700900100");
    }));

  test("setup accepts reordered flags and equals syntax", () =>
    withDirectory(async (directory) => {
      await setup(directory, ["setup", "--chat-id=447700900100", "--owner-id=447700900100"]);
      expect(await Effect.runPromise(whatsappSettings.load(directory))).toEqual({
        ownerId: "447700900100",
        chatId: "447700900100",
      });
    }));

  test.each([
    ["missing flag", ["setup", "--owner-id=447700900200"]],
    ["missing value", ["setup", "--owner-id=447700900200", "--chat-id"]],
    ["duplicate flag", ["setup", "--owner-id=447700900200", "--owner-id=447700900200"]],
    ["unknown flag", ["setup", "--owner-id=447700900200", "--chat-id=447700900200", "--unknown"]],
    ["extra positional", ["setup", "--owner-id=447700900200", "--chat-id=447700900200", "extra"]],
    ["positional setup", ["setup", "447700900200", "447700900200"]],
    ["positional link", ["link", "447700900200"]],
    ["missing link flag", ["link"]],
    ["missing link value", ["link", "--phone-number"]],
    ["duplicate link flag", ["link", "--phone-number=447700900200", "--phone-number=447700900200"]],
    ["unknown link flag", ["link", "--owner-id=447700900200"]],
    ["extra groups flag", ["groups", "--phone-number=447700900200"]],
    ["extra groups positional", ["groups", "extra"]],
  ])("rejects %s without changing saved settings", (_, args) =>
    withDirectory(async (directory) => {
      await setup(directory, ["setup", "--owner-id=447700900100", "--chat-id=447700900100"]);
      const path = join(directory, "channels/whatsapp.json");
      const before = await readFile(path, "utf8");
      await expect(setup(directory, args)).rejects.toHaveProperty("reason", "configuration");
      expect(await readFile(path, "utf8")).toBe(before);
    }),
  );

  test.each([
    [
      "a private chat that is not the owner's",
      ["setup", "--owner-id", "447700900100", "--chat-id", "447700900200"],
    ],
    [
      "a number with a plus sign",
      ["setup", "--owner-id", "+447700900100", "--chat-id", "447700900100"],
    ],
    [
      "a chat that is not a group JID",
      ["setup", "--owner-id", "447700900100", "--chat-id", "120363@broadcast"],
    ],
    ["an unknown command", ["configure", "447700900100", "447700900100"]],
    ["a link without a valid number", ["link", "--phone-number", "07700 900100"]],
  ])("rejects %s without saving", (_, args) =>
    withDirectory(async (directory) => {
      await expect(setup(directory, args)).rejects.toThrow();
      expect(await Effect.runPromise(whatsappSettings.load(directory))).toBeNull();
    }),
  );
});
