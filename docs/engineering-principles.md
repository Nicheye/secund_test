# Engineering principles

Rules for this project, adapted from common code-review feedback. They are language-agnostic
because the stack is not chosen yet. Where `AGENTS.md` disagrees, `AGENTS.md` wins.

Ordered by how often each area comes up in review.

---

## 1. Core principles

1. **Simpler is better.** Every variable, flag, abstraction, helper and dependency must justify
   itself. The default review question is "Is it required?"
2. **Names say exactly what things are**, in the domain's own vocabulary.
3. **One concept, one name, one source of truth.** No synonyms, no duplicated constants or helpers.
4. **Explicit over implicit; flat over nested.** No hidden state, magic numbers or strings, or
   logic inferred from key order or formatted output.
5. **Fix root causes, not symptoms.** No sleeps, retries or casts that hide a bug. If a hack is
   unavoidable, contain it at one call site and comment why.
6. **Fail loud for developers, safe for users.** Never swallow exceptions that signal real bugs.
   Clients get stable, documented error responses; logs get the detail.
7. **Think "what happens when...":** null, empty, zero, many; duplicate request; retry after
   timeout; two requests at the same instant; insufficient funds; unknown currency; deleted or
   foreign account; a hundred times more load.
8. **Write code as if from scratch knowing all requirements.** Refactor properly; don't carry
   old workarounds over.
9. **YAGNI, with cheap foresight.** Don't add structure for the future, but shape signatures so the
   obvious next step doesn't need a rewrite.

---

## 2. Fintech rules (highest priority for this project)

These are what a reviewer of a money-handling service checks first.

- **Money is never a float.** Store and compute in integer minor units (cents) or an exact
  decimal type. Amount and currency travel together as one value; never add or compare amounts of
  different currencies. Define rounding mode and scale once, in one place.
- **Amounts are validated at the boundary:** positive for transfers, within limits, valid currency
  code from a known set.
- **Idempotency on every write endpoint.** Clients send an idempotency key; the same key with the
  same payload returns the original result, the same key with a different payload is rejected.
  Enforce it with a unique constraint, not an in-memory check.
- **Atomicity.** A transfer's debit, credit and ledger entries commit in one transaction or not at
  all. No partial state is ever observable.
- **No lost updates, no double spend.** Concurrent operations on one account must be serialized
  (row lock, optimistic version check, or serializable isolation). Acquire locks in a consistent
  order (e.g. by account id) to avoid deadlocks. Document the choice and its trade-offs.
- **Ledger is append-only.** Never update or delete a posted entry; corrections are new
  compensating entries. Prefer double-entry: every transfer is balanced debits and credits.
  Balance is either derived from the ledger or provably consistent with it.
- **State machines are explicit.** Transaction states (e.g. pending, completed, failed, reversed)
  and allowed transitions live in one place; illegal transitions raise errors.
- **Auditability.** Every state-changing operation records who, what, when and a correlation id.
- **Never log secrets or sensitive data** (full card numbers, tokens, credentials). Mask
  identifiers where needed.
- **Time:** timestamps are UTC, timezone-aware; intervals are half-open `[from, to)`.
- **IDs:** opaque and non-sequential for anything exposed to clients (UUID or similar).
- **Authorization on every operation.** Access rules live in one place; a missing or foreign
  object returns 404, never another user's data.
- **Secure defaults:** no fallback to the most permissive mode; validate against known values.

---

## 3. Naming

- **Name = content.** Type, plurality and meaning match: `accountIds` holds ids, `account` holds
  an account. A misleading name is worse than a long one.
- **Use existing domain terms; never invent new ones.** Pick one of account/wallet,
  transfer/payment, balance/funds, remove/delete, and use it everywhere: code, API, DB, docs.
- **Symmetry:** paired names agree (`debit` / `credit`, `fromAccount` / `toAccount`).
- **No redundancy with context:** `Transfer.status`, not `Transfer.transferStatus`; no type
  suffixes like `List` or `Array`; no `I`/`T` prefixes; don't leak implementation into names.
- **No abbreviations** except the universal (id, url, http, json, db).
- **Booleans** are `is`/`has` or adjectives, named after the non-default state, and positive
  (`isFrozen`, not `isNotActive`).
- **Functions** start with verbs; `create`/`delete`/`add` mean what they actually do.
  Meaningless words: `data`, `info`, `item`, `handle`, `process`, `manager`, `helper`.
- **Constants** UPPER_CASE; **enums** are used instead of string literals; event and status
  names are past participles or clear states (`TRANSFER_COMPLETED`).
- **Test names** state the exact behaviour and match the assertion
  (`rejects_transfer_when_balance_is_insufficient`).
- If a naming question came up once, it will come up again: fix the name or comment.

---

## 4. Testing

- **A test must be able to fail.** Remove the code under test and it goes red. Filter tests need
  distractor data. No tautological assertions.
- **Bug fixes start with a failing test** that reproduces the bug.
- **Every behaviour is tested, including negative and edge cases:** insufficient funds, zero and
  negative amounts, same-account transfer, unknown account, currency mismatch, duplicate
  idempotency key, illegal state transition.
- **Concurrency test is mandatory:** many parallel transfers on shared accounts must preserve the
  total balance and never allow a negative balance. Idempotency under parallel duplicates too.
- **Assert against storage directly**, not only through the API, and write expected values
  literally (whole-object equality, explicit numbers) with no logic in assertions.
- Test only what's relevant; make preconditions explicit; use realistic neutral data.
- **One bug should fail one test.** Separate pure logic tests from the thin API wiring tests.
- Use parametrization for case matrices.
- **Mock only third-party boundaries** (external payment providers, email, clock), never our own
  functions. Freeze time instead of sleeping.
- Arrange / Act / Assert separated by blank lines. Don't delete existing tests when refactoring.
- Never bend business logic to make tests easier; use fixtures or configuration.

---

## 5. Data layer

- Constraints live in the database as well as in code: `NOT NULL`, `UNIQUE` (idempotency key),
  `CHECK` (non-negative balance where applicable), foreign keys with deliberate delete behaviour.
- Justify every nullable column and every index.
- Avoid N+1 queries: aggregate in the database, hoist loop invariants, batch updates.
- Prefer upsert/`get or create` primitives that are atomic over check-then-insert.
- Migrations are sequential, minimal, and match the models. New columns on populated tables are
  nullable first, then backfilled. Data migrations are idempotent.
- Repository/query objects own access rules and filtering; business logic doesn't build ad-hoc
  queries.

---

## 6. API design

- Resource-oriented REST, consistent URL style, correct HTTP status codes
  (400 validation, 401/403 auth, 404 missing or foreign, 409 conflict or idempotency mismatch,
  422 business rule violation as the project chooses and documents).
- One error response shape across the API, with a stable machine-readable code and a message.
- Validation happens in a dedicated layer at the boundary, not scattered through business logic.
- Amounts in responses are explicit about scale and currency; datetimes are ISO-8601 UTC.
- List endpoints are paginated and have a stable order.
- Don't expose fields clients don't need. Version breaking changes; keep clients working.
- Write endpoints accept an idempotency key (see section 2).

---

## 7. Architecture and style

- **Right module, right layer:** transport (HTTP) -> service (business rules) -> persistence.
  Business logic does not know about HTTP; persistence does not contain business decisions.
- Separate business logic from invocation concerns (retries, queues, scheduling).
- Prefer functions over classes unless modelling a real thing. Prefer composition.
- Early returns; shallow nesting; readable over clever; lookup tables over long branching.
- Catch specific exceptions only; never a bare catch; domain errors are typed.
- Immutability by default; avoid shared mutable state.
- Configuration via one config module; no scattered environment reads; no secrets in the repo.
- Dependencies must be justified: standard library first, official SDKs over hand-rolled
  clients, no library for a single call.
- Imports at the top of the module; relative within a package, absolute across packages.

---

## 8. Comments, docs and hygiene

- **Comments** in English, only for the non-obvious *why*: concurrency choices, rounding, trade-offs.
  No restating code, no TODOs, no history, no commented-out code.
- No debug prints, placeholder data, or stray files in a change.
- The README explains how to run, how to test, key design decisions and their trade-offs, what
  was deliberately left out, and what would come next. Treat it as part of the deliverable.

---

## 9. Process and git

- One concern per change; revert unrelated edits (formatting, lockfiles, renames).
- Review your own diff before asking anyone else to.
- Never commit to `main` directly; short lowercase-hyphen branch names; no `git add .`.
- Commit messages and PR titles say what truly changed.
- CI runs lint, type check and tests; keep scripts as simple as possible.

---

## 10. Review taste

- Pet peeves: synonyms and invented terms; single-use variables; magic numbers; sleep-based
  fixes; defensive checks for impossible states together with missing checks for real nulls;
  tests that pass for the wrong reason; hidden state; leftovers.
- Ask "What if...?" and "Why?" and attach a concrete suggestion.
