# JSON

Provisional, 2026-10-03. The design of `skein-json`: a bounded JSON
tokenizer, pulled by demand, and a sized writer. It is a step machine of
a connection's stack (programming-model.md, 4), and depends on lib only.

## 1. In one page

- **Tokens, not documents.** The tokenizer turns bytes into a stream of
  tokens, by demand. An application decodes its own documents from the
  tokens, with small state machines, into its domain's types.
- **Bounded everywhere.** Nesting is held in a `lib::Stack` of configured
  depth, never in recursion; strings are under a maximum length.
- **No floats.** Numbers go up as validated text; the consumer parses the
  integers it expects, with checks.
- **The writer is sized:** measure the document first, then write it,
  escaped, into a box of exactly its length.

## 2. In skein

`skein-json` depends on lib only. A service's protocol layer stacks it
over whatever carries the documents (an HTTP body, server-sent events'
data) and under its own decoders (http.md, 2). It has its own `Limits`
and `worst_case`, and each entry point declares its `MAX_OUT`.

## 3. The tokenizer

- **Pulled by demand:** it demands what it needs from the stream below,
  and emits a token when the side above has room for it.
- **Nesting** is held in a `lib::Stack` of configured depth, since the
  depth of nested input is the peer's choice (programming-model.md,
  section 8). Past the depth, the document is refused.
- **Strings** are unescaped and checked as UTF-8, into exact-size
  `Box<[u8]>`s under a maximum length.
- **Numbers** go up as validated text, never as floats.

## 4. The writer

Measure first, then write with escaping into `Writer::new(len)`. A
document's length is the writer's own computation, so every write fits,
and finishing short is an assertion.

## 5. Decoding is the application's

An application decodes its own documents with small state machines over
these tokens, in its own protocol layer, into its domain's types.
Structure the domain acts on is decoded on the way in, all of it: a tool
call inside an LLM's answer reaches the domain as a typed call, not as
JSON to be sent back down for decoding later (programming-model.md, 4).

## 6. Testing

- **Machine worlds** (testing-strategy.md, 2.4), with documents cut at
  random, transcripts of real documents (an LLM provider's tool calls, a
  forge's API), generated documents, and hostile ones: nested too deep,
  strings too long, invalid UTF-8, numbers that are not numbers.
- **The writer against the tokenizer:** what one writes, the other reads
  back the same.
- **Fuzzing:** one target, fed `Bytes` under every demand.

## 7. Open questions

- **Decoding by hand** into an application's types is verbose without
  serde or traits. If that hurts, the candidate is a generator that turns
  a schema into plain step code at build time, with its output checked in
  and reviewed. Procedural macros stay out.

## 8. Not built yet

All of it. temper pulls the tokenizer first, for the agent's LLM client,
and the writer for the fake LLM provider.
