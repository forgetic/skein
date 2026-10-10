# Codecs from schemas

Provisional, 2026-10-06, revised 2026-10-09. How an application protocol
built on skein encodes its records. A schema says each record's fields
and their bounds. skein's generator turns it into plain Rust codecs,
limits, worst cases and golden bytes, and `skein-codec` holds the little
the generated code shares. Codecs carry the bodies of a framed channel
(channel.md), and records kept in files or stores.

## 1. In one page

- **The schema is the specification.** A small file per family and version
  gives each record's fields, their types and their bounds. Design
  documents name records and bounds; the schema gives the bytes.
- **Generated, not written.** A generator, ordinary Rust in skein, turns a
  schema into:
  - the records' types, with their bounds sealed by constructors;
  - limits, whose ceilings are the schema's bounds;
  - measured encoding;
  - bounded decoding with typed problems;
  - each record's worst case;
  - golden bytes.
- **Plain Rust of the subset** (programming-model.md, section 10):
  structs, enums, functions and `match`; no traits beyond derives, no
  generics, closures or macros. The output is kept in the consumer's
  repository and reviewed like any other code. A test fails when the code
  and the schema drift.
- **Sized** (programming-model.md, section 8). Every length comes before
  its bytes, and is checked against its limit and what is left before
  anything is set aside. Types form a tree of fixed depth, so decoding
  never recurses on a peer's choice.
- **Canonical.** One value has one encoding, and decoding refuses any
  other, so what decodes encodes back to the same bytes.
- **Versioned.** A schema is one version of a family, and a family has
  one current version. A record kept beyond a channel carries its
  version. A reader reads the current version and refuses another;
  nothing is translated.

## 2. In skein

- **`skein-codec`** depends on lib only. It holds what the generated code
  shares:
  - the reasons a decoding fails;
  - bounded reads of lengths, counts and text;
  - reading a versioned record's version.
- **`skein-codegen`** is the generator: ordinary Rust, not step code. A
  consumer depends on it in its tests only.
- **A consumer's codec crate** holds its schemas, the generated code, its
  golden bytes and its drift test, and depends on lib and `skein-codec`.
  smith's `smith-charter`, `smith-channel` and `smith-transcript` are such
  crates.

## 3. The schema

```
family smith-charter 1

record Section {
    title: text 256
    body: text 65536
}

enum Effect { read, write }

record HostTool {
    name: text 64
    description: text 4096
    input: bytes 65536
    effect: Effect
    deadline: duration
}

record Report {
    fields: list 32 Field
}

enum Form {
    report: Report
    failure
}
```

| Type | Encoded as | In Rust |
|---|---|---|
| `u8`, `u16`, `u32`, `u64` | fixed width, big-endian | the same |
| `bool` | a byte, 0 or 1 | `bool` |
| `duration` | a `u64` of nanoseconds | `lib::Duration` |
| `fixed N` | N bytes | `[u8; N]` |
| `bytes N` | a `u32` length, then at most N bytes | `Box<[u8]>` |
| `text N` | a `u32` length, then at most N bytes of UTF-8 | `Box<[u8]>`, checked UTF-8 |
| `list N T` | a `u32` count, then at most N items | `lib::List<T>` |
| `option T` | a byte, 0 for none or 1 for some, then the value | `Option<T>` |
| a record | its fields, in order | a struct |
| an enumeration | a byte tag, the variants numbered in order from 0, then the variant's record, if it has one | an enum |

- **Durations, never times.** A time means something only on its writer's
  clock. A peer turns a duration into a deadline on its own clock
  (programming-model.md, section 9).
- **Every field is present.** An absent value is an explicit `option`;
  there are no defaults.
- **A variant holds one record, or nothing.** The record seals its own
  bounds, which an enum variant's fields could not.
- **A type uses only types declared before it.** The types form a tree, and
  decoding nests to a fixed depth.
- **No imports.** A record of another family, such as a charter carried in
  a channel's body, travels as a `bytes N` field and is decoded by its own
  family's code.
- **A record marked `versioned`** starts with its family's version, a
  `u16`. A reader of bytes kept elsewhere learns from it which code reads
  them.

## 4. What is generated

For each family:

- **Its limits.** A plain struct with one limit per bound in the schema,
  and a constant holding the schema's bounds as ceilings. A side's limits
  may be smaller than the ceilings, never larger (programming-model.md,
  section 7).
- **Its problems.** An enumeration of the family's fields, and a problem
  that names one of them with `skein-codec`'s reason.

For each record and enumeration:

- **Its type.** Fields are private:
  - a constructor checks every field against the limits it is given and
    refuses with the field that broke one, so any value that exists fits
    them;
  - accessors read the fields, and a function that consumes the value
    hands its fields back, so bytes move and are not copied;
  - the derives are `Debug`, `Clone`, equality and `Hash`.
- **Measuring:** its encoded length. This cannot overflow, since the
  ceilings bound it.
- **Encoding** into a `lib::Writer` with that much room left: the rest of a
  frame's writer (channel.md, section 7), or a box of its own.
- **Decoding** from a `lib::Reader`, against the limits it is given. It
  refuses with a problem naming the field:
  - too short;
  - beyond a limit;
  - an unknown tag;
  - a `bool` that is neither 0 nor 1;
  - text that is not UTF-8;
  - another version;
  - bytes left over at the end.

  Every box is allocated at its final length, once that length has passed
  its limit and what is left.
- **Its worst case,** for given limits:
  - the most bytes it encodes to, which must fit a `u32` at the ceilings or
    the schema is refused;
  - the most heap its decoded value holds, as a function over the limits
    and the sizes of the generated types, which only the compiler knows.

## 5. Versions

- **Any change a reader would notice is a new version:** a new schema,
  whose generated code replaces the old. There is one version at a time,
  and no promise of stability to anyone outside the family's owner.
- **A reader reads one version.** It reads a versioned record's version
  first, with `skein-codec`, and refuses another with the problem
  *another version*. Nothing is translated: what an owner does with a
  record of another version, such as starting afresh, is its own policy.
- **A writer writes its current version only.**
- **On a channel,** the agreed version picks the code for its bodies
  (channel.md, section 5.2). Bodies carry no version of their own.
- **A version's golden bytes stand for it.** A change that moves them is
  a new version.

## 6. Golden bytes and tests

- **Golden values follow a fixed rule,** so the generator can change
  without moving them. For each record, and each variant of each
  enumeration, there are two values:
  - **the smallest:** integers zero, bytes, text and lists empty, options
    absent, each enumeration at its first variant;
  - **a full one:** integers at their maximum, bytes and text a fixed
    pattern of a few bytes, each list with one full item, options present.

  Their encodings sit in a file beside the code, and each type's values
  must encode and decode as recorded.
- **Bounds:** generated tests decode a field at its limit, and one past it,
  without stored files.
- **Drift:** a test runs the generator on the schema and compares the
  result with the committed code and golden bytes. On a difference it
  fails, saying how to write them again, which the same test does when an
  environment variable asks it to.
- **Generated files** say so in their first line. They are left out of
  formatting, and pass the workspace's lints with no allowances.
- **Fuzzing,** in the fuzzy suite: each top-level record's decoder over
  arbitrary bytes. It never panics, and what decodes encodes back to the
  same bytes.
- **Memory:** decoding a full value at the ceilings, measured with the
  counting allocator against its heap's worst case.
- **The generator's own tests:**
  - schemas it refuses: a type used before it is declared, a name used
    twice, a worst case too large;
  - small schemas' encodings, compared with byte strings written by hand.

## 7. Open questions

- **Maps and sets:** sorted lists whose keys are checked unique, when a
  record needs one.
- **Smaller integers:** variable-width encodings, if a payload shows that
  fixed widths cost too much.
- **JSON from schemas:** the same generator writing JSON codecs over
  `skein-json` (json.md, section 8).
- **Other languages:** a generator for a peer not built on skein, if one
  appears. The schema is plain enough for it.
