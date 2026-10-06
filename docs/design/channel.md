# Framed channels

Provisional, 2026-10-06. `skein-channel` is a framed channel between two
peers over a stream, for the application protocols that services built on
skein design for themselves. An application gives it two things:

- a schema, as data: its magic, its versions and its kinds;
- codecs for its bodies (codec.md).

The channel gives it framing, an opening that agrees a version, bounds and
flow control. Its first users are smith's channel between a host and an
agent, and temper's between its engine and its workers. This design
replaces the earlier one, which kept temper's first channel as it was.

## 1. In one page

- **A protocol machine** (programming-model.md, section 4). It depends on
  lib only and keeps no timers. The stream below may be a pipe, a socket
  or TLS, and the machine cannot tell which.
- **Generic.** The application's schema names its kinds, and its codecs
  give bodies their meaning. The channel knows no calls, no payloads, no
  phases beyond its opening, and nothing of what a credential proves.
- **Frames:** a fixed header with a kind, flags and a length, then a body.
  Every length is checked before anything is set aside for it.
- **An opening agrees a version.**
  - The initiator offers a range of versions and an opaque credential.
  - The responder's owner accepts, with a version both speak, or refuses.
  - Each side states the largest body it takes of each kind, and each owner
    hears the other's.
  - Only then do the owners exchange frames of their own.
- **One stream, or one each way.** The machine reads from one stream and
  writes to one, which may be the same. Writing never waits for reading,
  and with a stream each way, either may fail or end while the other goes
  on.
- **Reading is a demand:** one frame at a time, when the owner has room
  for it.
- **Writing is measured** and queued against an output cap. What does not
  fit is refused at the entrance, and the owner can see the room left.
- **The owner keeps time and policy.** The machine says what it is waiting
  for and what drained. The owner arms the opening's deadline, keepalive,
  silence and stall, and decides on credentials and on the peer's terms.
- **A last word.** Once the owner finishes or refuses, nothing more is
  sent.

## 2. In skein

- **Dependencies:** `skein-channel` depends on lib only. A service's
  protocol layer owns one machine per channel, or a component of that layer
  does, as smith's halves do.
- **What the owner does:**
  - makes the streams, from io or TLS, and closes them;
  - encodes and decodes bodies with its codecs (codec.md);
  - translates the bodies to and from its domain's vocabulary.
- **What stays outside the channel:**
  - deciding on a credential, and on the peer's terms;
  - timers;
  - the meaning of bodies;
  - calls and their answers;
  - reconnecting.

## 3. Frames

```
offset  size  field
0       2     kind     u16, big-endian
2       2     flags    u16, zero in every version so far
4       4     length   u32, big-endian: the body's length, header excluded
8       ...   body
```

- **The header is frozen.** Every version, of every application, reads it
  the same way.
- **Flags must be zero** unless a feature agreed at the opening (5.1) gives
  them a meaning.
- **Kinds come in two ranges:**
  - **`0x0001`–`0x00ff` are the channel's own** (5.1). The same for every
    application, they never change meaning.
  - **`0x0100`–`0xffff` are the application's.** Each kind belongs to one
    direction: from the initiator, or from the responder.
- **Bodies** are the application's records, encoded by its codecs. The
  channel reads a body as bytes and hands it up whole.

## 4. The schema

What an application gives the machine, as data, checked when the machine
is made:

- **its magic:** four bytes naming the application's protocol;
- **its versions:** the contiguous range this side speaks;
- **for each version, its kinds:** for each kind, its direction and the
  largest body this side takes or may send of it, from the owner's limits;
- **the role:** initiator or responder, for this machine;
- **its limits** (section 10).

The machine refuses a schema in which:

- a kind appears twice in a version;
- an application kind lies in the channel's own range;
- a version in the range has no table;
- a kind's largest frame does not fit the output cap.

## 5. Opening

### 5.1 The channel's messages

| Kind | Name | Body |
|---|---|---|
| `0x0001` | `Open` | magic, 4 bytes; lowest version `u16`; highest version `u16`; features `u16`; credential, bytes |
| `0x0002` | `Accept` | version `u16`; features `u16` |
| `0x0003` | `Refuse` | reason `u16`; text, bytes |
| `0x0004` | `Terms` | a list of (kind `u16`, largest body `u32`) |
| `0x0005` | `Ping` | nothing |
| `0x0006` | `Unsupported` | kind `u16` |

- **Layouts are frozen,** for every application and every version. Bytes
  and lists are a `u32` length, then their contents.
- **Features** are the channel's own extensions, such as a meaning for the
  header's flags. `Open` offers them, and `Accept` names those taken. None
  are defined, so both are zero today.
- **A refusal's text** is for operators, at most 256 bytes, so a refusal
  stays small and fixed in size (programming-model.md, section 8).
- **Reasons** `1` to `255` are the channel's:
  - `1`, *version*: no version in common;
  - `2`, *limits*: the peer's terms do not suit this side;
  - `3`, *framing*: the peer broke the framing, including a wrong magic.

  Reasons from `256` are the application's, named in its own design. A
  reason the reader does not know is reported as it came.

### 5.2 The sequence

```
initiator                                    responder
Open { magic, versions, features, credential }  ───►
                     ◄───  Accept { version, features }, then Terms
                     ◄───  or Refuse { reason, text }
Terms                                           ───►
(each owner hears ready, with the version and the peer's terms)
```

- **The responder's machine checks `Open`:** the magic, then the versions.
  - A wrong magic is refused with *framing*.
  - A malformed range (`lowest` above `highest`) is refused with
    *framing*.
  - A range with no version in common is refused with *version*.

  None of these asks the owner.
- **Otherwise the owner decides.** It hears of the opening, with the
  credential and the versions both speak. It accepts with one of them,
  normally the highest, or refuses with a reason of its own: the
  credential is unknown, or it is busy. For an agent's pipes, the owner
  accepts at once.
- **Terms list the kinds a side receives:** the largest body it takes of
  each, at most its schema's. The responder sends its terms right after
  `Accept`, and the initiator answers with its own.
- **Ready.** Each owner hears that the channel is ready, with the version
  and the peer's terms, once it has the peer's terms and has sent its own.
- **Judging the peer's terms is the owner's.** The machine only applies
  them, refusing at the entrance a frame the peer does not take (section
  7). An owner that wants a mismatch to show at the opening, rather than
  on the first large frame, refuses with *limits*, naming the kind in the
  text. A peer may still refuse after this side has heard ready.
- **The opening's deadline** is the owner's. The machine says that it is
  opening, and the owner closes it when its deadline passes.

### 5.3 Which frames, when

- **`Open`** is the initiator's first frame. `Accept` and `Refuse` are the
  responder's answer to it.
- **`Terms`** come once from each side, as above.
- **`Ping`** may come from either side once `Open` has crossed.
- **`Refuse`** may come from either side at any time (8.1).
- **`Unsupported`, and the application's kinds,** come only after the
  sender has sent its terms.
- **Anything else is a framing error:**
  - `Open`, `Accept` or `Terms` after they were due;
  - an application kind before then;
  - kind `0x0000`, or an unknown kind in the channel's own range.

## 6. Reading

- **The machine reads the opening by itself.** After it, it reads only when
  the owner asks.
- **One frame at a time.** The owner asks for a frame when it has room for
  one more, and the read is answered once:
  - **body:** an application frame's kind and its body;
  - **ping:** the peer's `Ping`;
  - **unsupported:** the peer skipped one of this side's kinds;
  - **refused:** the peer's `Refuse`, with its reason and text;
  - **ended:** the stream ended between frames;
  - **closed:** the terminal (8.3).

  A full owner stops the reading, and so, in time, the peer's writing.
- **The header is checked first.** A known kind must come from the peer's
  direction, in the agreed version and phase (5.3). Its length must be at
  most what this side said it takes. Anything else is a framing error, and
  the machine refuses with *framing* (8.1).
- **The body** goes into one box of its length, filled by demands of at
  most a chunk each. The stream below then needs room for only one chunk,
  whatever the frame's size.
- **The owner decodes it.** A body that does not decode is the owner's to
  judge: it refuses with *framing*, or carries on.
- **Unknown kinds are skipped.** An application kind the agreed version
  does not have, with a length within the skip bound, is read and dropped
  in chunks, without allocating. The machine answers it with `Unsupported`
  and goes on reading for the owner's read (programming-model.md,
  section 8). The next header waits until the `Unsupported` is queued. A
  peer that keeps to the terms never sends such a kind.
- **Silence** is the owner's to time, and only while a read is
  outstanding: an owner that is not reading may hear nothing because of
  itself.

## 7. Writing

- **Measured, in one allocation.** The owner measures a body with its
  codec, asks the channel for a writer of that frame (header and body), and
  encodes the body into the rest of it. The frame moves down whole.
- **Checked at the entrance.** The machine refuses a frame, which the owner
  hears as unsent, with why:
  - its kind is not the owner's to send;
  - the peer did not list it in its terms;
  - it is larger than the peer takes;
  - the queue is full.

  An admitted frame is sent, meaning queued, not delivered. Both answers
  name the owner's token for the frame.
- **The output cap** is in bytes and frames. The owner sizes it from its
  domain's limits, and can ask how much room is left at any time, so it
  can keep part of it for what matters most and send the rest, such as
  facts, only while room remains. A peer that stops reading fills the cap,
  which the owner's stall deadline catches.
- **The channel's own frames have room of their own.** Beyond the cap,
  the machine keeps room for one `Refuse`, one `Unsupported` and one
  `Ping`.
  - At most one `Ping` waits at a time; a ping asked for while one waits is
    dropped. A ping has no token and no answer.
  - A `Refuse` displaces everything queued (8.1).
- **Writing never waits for reading.** The machine moves output down with
  lib.md, section 7.1's independent output, which io's sockets and write
  pipes provide, as does TLS's native face. It reads with read-only demands.
  On one socket both happen at once; with a pipe each way, each happens on
  its own pipe.
- **Drained** goes up each time a queued frame is handed below on a grant.
  The owner's stall deadline runs while frames are queued and resets on
  drained.

## 8. Refusing and ending

### 8.1 Refusing

Either owner may refuse at any time, with a reason, and the machine
refuses on a framing error. A refusal:

- stops reading;
- drops the queued frames;
- queues the `Refuse` in its own room;
- finishes (8.2).

The owner's deadline closes a channel whose refusal does not drain.

### 8.2 Finishing

Finishing is the owner's last word: nothing more is admitted after it, and
the output stream is finished once what is queued has gone.

- **With a stream each way,** reading goes on until the peer ends or the
  owner closes.
- **Over one stream,** lib.md's contract allows finishing only with no read
  outstanding (lib.md, section 7). The machine finishes once the read in
  flight is answered, or has crossed the end, and reads nothing after it.
  An owner that must read until the peer's end finishes after it hears
  ended.
- **The owner closes** a finished channel once it has heard what it needs.

### 8.3 Failures and the terminal

- **With a stream each way,** each fails alone:
  - **the write stream failing** goes up as *output failed*, and reading
    goes on, so a peer that exited after its last word is still read to
    the end;
  - **the read stream failing** ends reading, and writing goes on.
- **Over one stream,** a failure ends both.
- **Closing** stops now. The read is withdrawn, the output right is
  cancelled or released, and the queue is dropped. The owner hears closed
  once that right has settled; closing the streams is the owner's.
- **One terminal.** A channel is active, then closing, then closed, and its
  owner hears closed once, with why (programming-model.md, 5.2):
  - closed by its owner;
  - refused, by this side or by the peer, with the reason;
  - a framing error;
  - a stream failed;
  - the stream ended inside a frame, or before ready.

## 9. Entry points

| From the owner | What it does |
|---|---|
| open | the initiator's `Open`, with its credential |
| accept, refuse | the responder's owner's answer to an opening; refuse also at any later time |
| read | read one more frame |
| send | queue one frame, named by the owner's token |
| ping | queue a `Ping` |
| finish | the last word |
| close | stop now |

| To the owner | What it says |
|---|---|
| opening | a responder's opening: the credential and the versions both speak |
| ready | the version agreed, and the peer's terms |
| body, ping, unsupported, refused, ended | the answer to a read (section 6) |
| sent, unsent | a frame admitted, or refused with why, by the owner's token |
| drained | a queued frame was handed below |
| output failed | with a stream each way, the write stream failed |
| closed | the terminal, with why |

- **Below,** the machine speaks lib.md's stream: read-only demands to the
  stream it reads, and section 7.1's output to the stream it writes.
- **Bounds.** Each entry point has a most-outputs bound, which the owner
  reserves first.
- **Queries.** The owner can ask, at any time:
  - the room left in the queue;
  - what each side is waiting for: reading (the opening, a frame, nothing)
    and writing (frames queued, nothing). The owner's deadlines follow
    from these.

## 10. Limits and the worst case

| Limit | What it bounds |
|---|---|
| chunk | the most bytes of a body read at once |
| credential | the credential's bytes |
| skip | the largest unknown body skipped rather than refused |
| output bytes, output frames | the queue |
| kinds | the kinds of a version, and so the terms |

A refusal's text is fixed at 256 bytes (5.1).

The worst case per channel is the sum of:

- the intake below, a chunk plus a header, which the stream's owner keeps;
- one body being read, the largest this side takes;
- the output queue, at its cap, and the room for the channel's own frames;
- each side's terms;
- the machine's state.

## 11. Testing

- **A machine world** (testing-strategy.md, 2.4):
  - peer bytes cut at random, room granted late, streams that end early or
    fail, one at a time when there are two;
  - owners that read slowly, send at awkward moments, refuse and close in
    every state;
  - every refusal of the opening;
  - golden frames for the channel's own messages;
  - a fuzz target over the header, the opening and terms.
- **A protocol world** (testing-strategy.md, 2.5): two machines joined by
  in-memory streams, one socket-like and one a pipe each way.
  - What one owner sent is what the other received.
  - A slow reader stops the writer.
  - Both directions move at once.
  - A last word reaches the peer even when the writer's stream then fails.
- **Memory:** each world's channels, measured with the counting allocator
  against their worst case.
- **A scripted peer,** for services' worlds: it speaks the opening, then
  plays frames from a script and checks those it receives.

## 12. From the first design

- **Kept:**
  - the header;
  - an opening with a range of versions;
  - terms;
  - unknown kinds skipped and answered;
  - bodies read in chunks;
  - measured writes;
  - an output cap.
- **Gone:**
  - temper's roles (an engine, a worker's link, a worker's agent) and its
    first channel's behaviour kept for equivalence;
  - temper's magic and channel number;
  - a name and a secret as fields of their own, which become one opaque
    credential;
  - refusal codes for busy, unauthorized and replaced, now the
    application's;
  - a limits check inside the machine, now the owner's;
  - per-kind gates on the first message, which are the owner's phases;
  - the machine waiting on the owner's decoding, since the owner does not
    read again until it has decoded.

## 13. Open questions

- **Features:** compression, or a body continued across frames, when a
  user needs either.
- **Named calls:** correlation, a bound on calls in flight, one answer per
  call, withdrawal. Whether they are a layer above the channel in skein,
  once smith's and temper's channels both have them.
- **Reconnecting:** carrying an application's state across connections
  stays its owner's. temper's link redials, and smith's connected agent is
  still open.
- **TLS's server side** with independent output, for a service that
  accepts channels over TLS (tls.md, section 8).
