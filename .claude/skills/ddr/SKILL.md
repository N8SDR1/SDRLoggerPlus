---
name: ddr
description: Use when the user runs /ddr to get a pre-commit review of the currently staged Git diff in SDRLoggerPlus.
disable-model-invocation: true
---

# ddr — Pre-Commit Review of Staged Changes

Perform a read-only pre-commit review of SDRLoggerPlus, a C# Windows desktop radio-logging and SDR-adjacent application.

## Procedure

1. Run `git diff --cached` to get the staged diff. Also run `git diff --cached --stat` for an overview.
2. **If nothing is staged, stop immediately** and tell the user: "Nothing is staged — stage changes with `git add` before running /ddr." Do not review the working tree instead.
3. Review only code introduced, removed, or behaviorally affected by the staged diff. Read nearby unchanged code (with Read/Grep) when needed to understand the diff — but do not conduct a general codebase audit.
4. **Never modify files, stage changes, or commit.** This skill is strictly read-only.

## Review priorities (in order)

1. Correct behavior and data integrity
2. Logging accuracy
3. Reliability and recoverability
4. UI responsiveness and thread safety
5. Maintainability

## Check for

- Logic, state, boundary, and off-by-one errors
- Null-reference and object-lifetime risks
- Incorrect async, cancellation, synchronization, or exception behavior
- UI-thread violations, blocking work, deadlocks, races, and re-entrancy
- Incorrect or duplicated event subscriptions
- Resource leaks involving streams, files, timers, devices, subscriptions, or disposable objects
- Unsafe file I/O, partial writes, concurrent access, encoding, and serialization compatibility
- Culture-sensitive parsing
- Timestamp, time-zone, frequency-unit, rounding, and logging-accuracy errors
- Dropped, duplicated, or incorrectly ordered log records
- Settings load, save, migration, validation, and compatibility regressions
- Weak error handling that hides failures or leaves inconsistent state
- Meaningful readability, duplication, maintainability, and test gaps

### C# desktop concerns (pay particular attention)

- `async void` outside legitimate event handlers
- Unobserved fire-and-forget tasks
- Synchronously blocking asynchronous operations
- Updating UI-bound state from background threads
- Callbacks or timers surviving window shutdown
- Cancellation and disposal during shutdown
- Shared mutable state across callbacks or threads
- Incorrect `IDisposable` or `IAsyncDisposable` ownership

## Rules

- Distinguish newly introduced problems from pre-existing issues exposed by the change.
- Do not speculate or manufacture findings. Explain the concrete execution path behind every claimed defect.
- Order findings by severity and impact.
- Avoid duplicate findings with the same root cause.
- Do not report generic best practices.
- State when additional context is required to confirm a concern.
- Include replacement code only for small, unambiguous fixes.
- If no findings exist, explicitly say so.
- Use `Fix Before Commit` only when there is at least one substantiated must-fix issue.

## Severity definitions

- **Critical**: Likely crash, deadlock, data loss/corruption, materially inaccurate logging, or severe workflow regression
- **Medium**: A real correctness or reliability problem under plausible conditions
- **Low**: A worthwhile maintainability, clarity, resilience, or testability improvement

## Finding format

For every finding, output:

```
[Severity] Concise title
- Location: File, method/type, and changed lines when available
- Classification: Must fix or Should improve
- Confidence: High, Medium, or Low
- Problem: The specific defect or risk
- Impact: A realistic failure mode
- Evidence: The relevant execution path or changed behavior
- Recommended fix: The smallest safe correction
- Verification: A focused test or manual check
```

## Required ending

End with exactly:

```
Pre-Commit Recommendation
- Status: Approve | Fix Before Commit
- Must fix: <count and concise list, or "None">
- Nice to improve: <count and concise list, or "None">
- One-paragraph summary: <decision, highest-risk area, and what should be verified>
```
