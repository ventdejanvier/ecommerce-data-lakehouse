import assert from "node:assert/strict";
import test from "node:test";

import { resolveTrackingUserId } from "./tracking-identity.ts";


const identity = (overrides = {}) =>
  resolveTrackingUserId({
    explicitUserId: null,
    selectedPersonaUserId: null,
    authenticatedUserId: null,
    sessionId: "session_default",
    ...overrides,
  });


test("explicit payload user ID has highest priority", () => {
  assert.equal(
    identity({
      explicitUserId: " explicit-user ",
      selectedPersonaUserId: "persona-user",
      authenticatedUserId: "auth-user",
    }),
    "explicit-user",
  );
});

test("selected ML persona wins over guest identity", () => {
  assert.equal(
    identity({ selectedPersonaUserId: "persona-user" }),
    "persona-user",
  );
});

test("authenticated user wins over guest identity", () => {
  assert.equal(identity({ authenticatedUserId: "auth-user" }), "auth-user");
});

test("guest identity reuses and isolates browser session IDs", () => {
  const first = identity({ sessionId: "session_a" });
  const firstAgain = identity({ sessionId: "session_a" });
  const second = identity({ sessionId: "session_b" });

  assert.equal(first, "guest:session_a");
  assert.equal(firstAgain, first);
  assert.equal(second, "guest:session_b");
  assert.notEqual(first, second);
  assert.notEqual(first, "anonymous");
});

test("server-side identity resolution is safe without a session value", () => {
  assert.equal(identity({ sessionId: undefined }), "guest:server_session");
});
