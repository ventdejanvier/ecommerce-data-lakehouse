import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_RECOMMENDATION_USER_ID,
  resolveActiveRecommendationUserId,
} from "./tracking-identity.ts";


const identity = (overrides = {}) =>
  resolveActiveRecommendationUserId({
    explicitUserId: null,
    selectedPersonaUserId: null,
    authenticatedUserId: null,
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

test("selected ML persona wins over the shared fallback", () => {
  assert.equal(
    identity({ selectedPersonaUserId: "persona-user" }),
    "persona-user",
  );
});

test("authenticated user wins over the shared fallback", () => {
  assert.equal(identity({ authenticatedUserId: "auth-user" }), "auth-user");
});

test("anonymous users use the same recommendation fallback identity", () => {
  assert.equal(identity(), DEFAULT_RECOMMENDATION_USER_ID);
});
