# Purpose of This Document

This project implements several parts independently, including the API server,
relay, Worker, and search. Each part uses different languages and frameworks,
but **readers see them as one project**. If every part must be read differently,
the project as a whole feels fragmented.

This document therefore contains only **standards that can be followed in every
part**, not framework-specific rules. There are three criteria.

| Criterion | Question |
| --- | --- |
| Readability | Can a first-time reader immediately understand what the code does? |
| Usefulness of comments | Do comments contain what the code alone cannot explain, and are unnecessary comments absent? |
| Structural soundness | Does the code naturally belong where it is, with clear boundaries? |

Each item is written in a **verifiable form**, not merely as an instruction to
"follow the rule." When opinions differ, this document is the standard.

---

# 1. Comments

This is the most important section. More comments are not always better, and
fewer comments are not always better. Comments must contain **only what the code
cannot say**.

## Decision Rules

Comments serve three purposes. Delete a comment if it serves none of them.

| Purpose | Placement | Length |
| --- | --- | --- |
| Signpost | At each stage of a function with a long flow | One-line summary |
| Contract | At the top of a public function or class | Two or three lines |
| Rationale | Immediately above code that contains a decision | One or two sentences |

This does not mean "reduce comments." **Use signposts and contracts actively.**
Delete comments that merely translate a single line of code.

## Signpost Comments

- A one-line comment that translates the single line below it should be deleted.

Signposts break a multi-line flow into stages. They show the skeleton before the
reader examines the code.

- Keep them short and use **summary noun phrases** such as `~ lookup`,
  `~ validation`, `~ storage`, and `~ processing`.
- Do not add them when the flow is short and names already make it clear.

```
// Upload permission validation
...three lines...

// Version-number allocation and storage
...five lines...
```

## Contract Comments

For public functions and classes, document **what callers need to know**. State
what it does in one line, followed by conditions and cautions callers must
observe.

- Do not restate what the name already communicates.
- Instead of listing parameters, document constraints that names do not reveal.
- State what the function guarantees and what it does not guarantee.

```
/**
 * Claims and returns at most [limit] rows that are ready for publication.
 *
 * Selection and claiming use one statement, so another instance cannot intervene between them.
 */
```

## Rationale Comments

Where the code embodies a decision, record why that decision was made. The
following five categories qualify.

**1. Basis for a choice** — why this approach was selected

```
// Selection and claiming use one statement. Splitting them would allow another instance
// to claim the same row in between.
```

**2. Rejected alternatives** — why a seemingly easier approach was not chosen

```
// Do not wrap this in a transaction. It does not commit with the DB transaction,
// so its cost outweighs the benefit.
```

**3. Basis for a value** — why a number has that value

```
// The 10-second allowance adds a safety margin to the 5-second connection timeout.
// A 30-second value is mathematically incompatible with the demonstration settings.
```

**4. External constraints** — circumstances that were not decided by this team

```
// The API server owns this column's schema. Validate it here, but do not change it.
```

**5. Traps** — points where a literal reading of the code could mislead readers

```
// getLong returns 0 for SQL NULL instead of throwing. Check wasNull as well.
```

## Comments to Delete

- **A comment that repeats one line of code.** For example,
  `// Find by user ID` above `findByUserId(userId)`.
- **Commented-out code.** The history remains in Git.
- **TODO/FIXME without an owner or deadline.** If one remains, state what must be
  done, why, and by when.
- **Generated templates.** This includes documentation that only lists `@param`
  entries without explanation.
- **Change history.** For example, `// Updated 2026-08-12`.
- **Decorative separators.** For example, `// ===== Lookup starts here =====`.

## Comments to Fix

- **Comments that disagree with the code.** These are the most dangerous because
  they send readers in the wrong direction. Fix or delete them immediately.
- **Comments that are too long.** Reduce them to one or two core sentences. Move
  longer background explanations to documentation and add a link.
- **A single block containing several decisions.** Split it and place each part
  beside the relevant code.

## Style

The project does not read as one project if each part uses a different style.
Standardize on the following rules.

- **Use declarative, definitive sentences.** In Korean, use plain declarative
  verb endings. Do not use honorific endings or colloquial speech.
- **Write signpost comments as summary noun phrases.** For example, `lookup`,
  `validation`, and `storage`.
- **Do not speculate or hedge.** Replace phrases such as “this might happen” or
  “it may be better to” with definite statements. If something is uncertain,
  verify it before writing it down.
- **Do not include emotion or apologies.** Avoid phrases such as
  “unfortunately” or “there is no choice.”
- Write comments in Korean. Keep proper nouns and API names in their original
  form.
- Describe public functions and classes using the language's documentation
  comment syntax, such as `/** */` or `"""docstring"""`.

```
// Good
// A shorter value terminates the process before the active batch completes.

// Bad — hedged and colloquial
// It might be a good idea to make this value a little generous.
```

---

# 2. Readability

## Names

- Make names describe a **role**. Do not put the type or data structure in the
  name (`userList` → `users`, `dataMap` → `versionsByDocument`).
- Do not invent abbreviations. Use only abbreviations already established in the
  project.
- Name booleans after the state when true (`deleted`, `hasNext`, `canRetry`).
- Use verbs in function names and confirm that the verb matches the actual
  behavior. A query function that changes a value, or an `update` function that
  creates one, has a misleading name.
- **Use the same name for the same concept throughout the project.** Using
  different terms in each part fragments the whole. Use the following terms.

| Concept | Use | Do not use |
| --- | --- | --- |
| Document identifier | `documentId` | `docId`, `document_no` |
| Document version number | `versionNo` | `version`, `versionNumber` |
| Document version identifier | `documentVersionId` | `versionId` |
| Event identifier | `eventId` | `messageId` |
| Tenant identifier | `tenantId` | `orgId` |
| Trace identifier | `traceId` | `requestId` |

When a new shared concept appears, add it to this table and align the parts.

## Functions

- A function does one thing. If its description requires "and," that is a place
  to split it.
- Keep a function at **one level of abstraction**. Do not mix lines that explain
  the business flow with lines that slice bytes.
- Keep nesting shallow. Handle exceptional cases first and return early to keep
  the main body flat.
- Group parameters when there are too many. Consecutive parameters of the same
  type are particularly easy to pass in the wrong order at call sites.
- Minimize public visibility. Expose only things that can be used independently
  from outside.

## Values

- Do not hard-code magic numbers. Extract them into named constants or
  configuration.
- Put operational values such as time, size, and counts in configuration.
  Provide defaults and document the rationale in comments.
- Do not hard-code addresses, paths, or credentials. Read them from environment
  variables.

## Things to Delete

- Unused functions, classes, variables, and imports
- Unreachable branches
- Code left behind from experiments
- Functions with different names but identical behavior

Readers assume code exists for a reason. Leaving unused code breaks that
assumption.

---

# 3. Structure

## Boundaries

- **Separate what your part owns from what other parts own.** Do not change
  schemas, contracts, or data owned elsewhere from your part. Validate them, and
  make mismatches visible.
- When changing a contract between parts—an event shape, API response, or
  database schema—coordinate with the other part before submitting the change.
- In code that depends on a contract, document **who owns that contract**.

## Organization

- Group files by feature first, then divide them by role within the feature.
- Split a file when it begins to contain multiple concerns.
- Put concepts shared by several parts in a common location; keep concepts used
  in only one place beside that usage.
- Keep dependencies flowing in one direction. If two components call each other,
  the structure is wrong.

## Layers

The names may differ between parts, but the roles are the same.

| Role | Does | Does not do |
| --- | --- | --- |
| Input/output | Receives requests or messages, validates their shape, and passes them on | Business decisions, storage |
| Business | Determines flows, validates rules, and changes state | Input/output format handling |
| Storage | Reads and writes | Business decisions |

- Move business decisions out of the input/output layer.
- If the business layer must know the request format, the boundary is drawn
  incorrectly.
- Move business-rule decisions out of the storage layer.

## Duplication

- Consolidate the same logic when it appears for the third time. Leave it as-is
  up to two occurrences.
- Do not force together code that only happens to look alike. Consolidate only
  what will change together.

---

# 4. Failure Handling

Readers must be able to determine what happens when the code fails.

- At external boundaries—database, HTTP, message broker, and files—**distinguish
  missing values from defaults**. Do not let an absent value become `0` or an
  empty string.
- **Do not allow silent incorrectness.** Code that lets an incorrect value flow
  through without an error or log is the worst kind.
- Watch for paths that return "zero results" instead of an error when a condition
  is violated. Zero results do not look like an error.
- Do not catch and swallow exceptions. If swallowing is necessary, explain in a
  comment why it is safe.
- **Prevent startup** instead of running with invalid configuration.
- Do not put sensitive information or complete request bodies in logs or error
  messages.

---

# 5. Tests

- Test names describe the **situation and expected result**. Do not use
  implementation phrases such as `calls ~`.
- If the name is insufficient, add a comment explaining what would be wrong if
  the assertion failed.
- After fixing a defect, **verify that the test actually fails when the fix is
  reverted**. An unverified test does not prevent regression.
- Tests that change shared-resource configuration must create dedicated
  resources so they do not affect other tests.
- When a test fails intermittently, run it the same number of times before and
  after the change before concluding the cause.

---

# 6. Commits and PRs

- Use the commit-message format `{type}: {Korean summary}` with `feat`, `fix`,
  `docs`, `chore`, or `refactor`.
- Make the summary communicate **what the problem was**, not just what was
  changed.
- Split commits and PRs for changes with different causes. Group only changes
  that cannot exist without one another.
- Do not mix cleanup—comments, names, dead code—with behavior changes in one
  commit. Mixing them makes review impractical.
- In a PR body, describe the problem, how to reproduce it, what was changed, and
  how it was verified.

---

# Pre-submission Checklist

Each part owner checks their own part.

**Comments**

- [ ] Removed comments that merely repeat the code
- [ ] Removed commented-out code
- [ ] No comments disagree with the code
- [ ] Rationale is documented for numeric values such as timeouts, limits, and sizes
- [ ] Contract-dependent code identifies the owning part

**Readability**

- [ ] Names describe roles
- [ ] Shared concepts use the terminology table
- [ ] Unused code and imports are removed
- [ ] Magic numbers are extracted into constants or configuration
- [ ] No hard-coded addresses or credentials remain

**Structure**

- [ ] Layer responsibilities are not mixed
- [ ] This part does not modify something owned by another part
- [ ] There are no circular call dependencies

**Failures**

- [ ] No path fails silently or produces silently incorrect output
- [ ] Every swallowed exception has an explanation

**Tests**

- [ ] Test names describe the scenario and expected result
- [ ] Tests accompanying defect fixes actually catch the regression
