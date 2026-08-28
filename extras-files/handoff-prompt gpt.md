We’re about to continue this work in a new chat. Create a handoff packet that preserves the important context, nuance, decisions, and current direction of this conversation so a new chat can pick it up with minimal loss.

Do not give me a generic summary. Build a practical restart brief.

Output it in these sections:

1. Core goal
- What we are actually trying to do, in plain language.

2. Current state
- Where the work stands right now.
- What has already been done.
- What remains unresolved.

3. Key context and constraints
- Important background facts, assumptions, definitions, preferences, and boundaries that matter to the task.
- Include only context that is still relevant.

4. Decisions made
- List the main decisions or conclusions reached so far.
- For each one, include the reasoning behind it, not just the conclusion.

5. Rejected paths / dead ends
- What we considered and ruled out, and why.
- Include mistakes, false starts, or approaches that caused problems.

6. Important nuance
- Capture any subtle framing, tradeoffs, tone requirements, edge cases, or “this only works if you remember X” details that would usually get lost in a normal summary.

7. Open questions
- What is still undecided, ambiguous, or needs to be checked next.

8. Best next step
- What the new chat should do first, based on everything above.

9. Ready-to-paste restart prompt
- Write a clean prompt I can paste into a new chat that tells the new assistant exactly how to continue from here without repeating work.

Rules:
- Separate confirmed facts from guesses or working assumptions.
- Preserve nuance over brevity, but stay concise enough to be practical.
- Do not flatten disagreements or uncertainty into fake certainty.
- If exact wording, examples, or snippets matter, include them briefly.
- Write this so a new chat can actually continue the work seamlessly, not just understand it.
Then in the new chat I put something like this:

I’m continuing an existing piece of work from another chat. Treat the following as a handoff, not as background trivia. Read it carefully, preserve the decisions and constraints, and continue from the current state without restarting the whole process.

When you reply:
- briefly confirm the goal, current state, and next step you understand
- flag anything genuinely ambiguous
- then continue the work from the best next step
- do not re-summarise everything unless needed
- do not suggest starting over

Handoff:
[paste handoff packet here]
