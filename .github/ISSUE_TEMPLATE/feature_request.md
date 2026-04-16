---
name: Feature request
about: Suggest a new feature or enhancement
title: "feat: <short description>"
labels: ["enhancement", "needs-triage"]
---

## Problem statement

Describe the problem or limitation you are trying to solve. Focus on the *why*, not the *what*.

> Example: "When running 10+ server instances, the fixed 5-second reconnection delay makes recovery feel slow after a brief PostgreSQL restart."

## Proposed solution

Describe your desired outcome or the API change you have in mind.

> Example: "Add an `options.ReconnectionDelay` property (default: `TimeSpan.FromSeconds(5)`) so applications can tune the delay."

## Alternatives considered

Have you explored workarounds or alternative approaches? Why are they insufficient?

## Additional context

Any other information, references to similar implementations in other libraries, or links to relevant issues.
