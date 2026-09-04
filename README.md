# kafkaui

**Swagger UI for your Kafka topics.**

kafkaui generates an [AsyncAPI 3](https://www.asyncapi.com/) document for a NestJS service from
its Kafka decorators and producer calls, with payload schemas taken from the real TypeScript
types, and serves an interactive viewer from the running app the way `SwaggerModule` serves
OpenAPI.

- Consumers from `@KafkaTopic()` / `@KafkaCron()` handlers and `registerHandler()` bindings
- Producers from `clientKafka.emit()` and `emitToKafka()` call sites, with the message key
- JSON Schema for every payload, resolved by the TypeScript compiler (interfaces, enums, unions, `Record`, nested types)
- Topic names from the code's own declarations, re-resolved from the live `ConfigService` at runtime
- A merge step that joins every service into one platform document
- A self-contained HTML viewer with **Example Value** / **Schema** tabs and copy buttons

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Serving the UI from a NestJS app](#serving-the-ui-from-a-nestjs-app)
- [CLI reference](#cli-reference)
- [What gets scanned](#what-gets-scanned)
- [Where topic names come from](#where-topic-names-come-from)
- [Platform view](#platform-view)
- [CI wiring](#ci-wiring)
- [Programmatic API](#programmatic-api)
- [Compatibility](#compatibility)
- [Limits](#limits)
- [License](#license)

## Installation

Pinned release tarball (no git needed at install time, works in `node:*-alpine` images):

```bash
npm install https://github.com/mjaniko/kafkaui/releases/download/v0.1.2/kafkaui-0.1.2.tgz
```

Or as a git dependency, when git is available:

```bash
npm install github:mjaniko/kafkaui#v0.1.2
```

From a clone:

```bash
git clone https://github.com/mjaniko/kafkaui.git
cd kafkaui && npm install && npm run build
```

The package builds on install (`prepare`), so a git dependency works without a registry.

## Quick start

Generate the contract of one service and open the viewer:

```bash
npx kafkaui generate --project ./core --service core --out asyncapi.json --html kafka-docs.html
open kafka-docs.html
```

Output on stderr tells you what was found:

```
topic names from core/.env.sample (19 topic vars)
core: 32 receive, 48 send, 215 schemas
wrote asyncapi.json
wrote kafka-docs.html
```

## Serving the UI from a NestJS app

```ts
import { readFileSync } from 'fs';
import { join } from 'path';
import { KafkaDocsModule } from 'kafkaui';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // after your KafkaTopic decorators have been processed
  const asyncapi = JSON.parse(readFileSync(join(__dirname, '..', 'asyncapi.json'), 'utf8'));
  KafkaDocsModule.setup('/kafka-docs', app, asyncapi);

  await app.listen(3000);
}
```

| Route | Content |
|---|---|
| `GET /kafka-docs` | interactive viewer |
| `GET /kafka-docs-json` | AsyncAPI 3 document |

`setup` works with both the Express and Fastify adapters. At runtime it:

1. walks the instantiated controllers and providers and reads the topic metadata your
   `KafkaTopic` decorator stored (`__kafka-topic-candidate` by default) and the
   `MessagePattern` metadata it was rewritten into, so the document lists every consumer the pod
   is actually bound to;
2. resolves every channel's env var through the live `ConfigService`, so names match the
   environment the pod runs in;
3. adds producers and payload schemas from the generated document, when one is given.

Without a generated document it still serves the consumers discovered at runtime.

Options:

```ts
KafkaDocsModule.setup('/kafka-docs', app, asyncapi, {
  topicMetadataKey: '__kafka-topic-candidate', // key your decorator uses
  resolve: (envVar) => process.env[envVar],     // custom name resolution
  service: 'core',                              // label for runtime-discovered operations
});
```

## CLI reference

```
kafkaui generate --project <dir> --service <name> [--env-file <file> | --no-env]
                 [--tsconfig tsconfig.json] [--version <v>] [--out asyncapi.json] [--html doc.html]
kafkaui merge <a.json> <b.json> ... [--title "Event bus"] [--out platform.json] [--html platform.html]
kafkaui html <asyncapi.json> [--out doc.html]
```

| Flag | Meaning |
|---|---|
| `--project` | service root containing `tsconfig.json` |
| `--service` | name used in the document (defaults to the directory name) |
| `--env-file` | `.env`, helm `values.yaml` (flat `KEY: value`) or JSON to resolve topic names for one environment |
| `--no-env` | keep channels keyed by env var only |
| `--tsconfig` | alternate tsconfig path, relative to the project |
| `--version` | document version (defaults to the project's `package.json` version) |

`kafka-docs` is kept as an alias of the `kafkaui` binary.

## What gets scanned

The scanner uses the TypeScript checker, not regular expressions.

| Pattern | Operation | Payload |
|---|---|---|
| `@KafkaTopic('TOPIC_X') async handler(params: T)` | receive | `T` |
| `@KafkaCron('TOPIC_X', opts) async handler(params: T)` | receive | `T` |
| `consumer.registerHandler({ [config.get('TOPIC_X')]: this.fn.bind(this) })` | receive | first parameter of `fn` |
| `clientKafka.emit(config.getOrThrow('TOPIC_X'), { key, value })` | send | type of `value`, `JSON.stringify` unwrapped |
| `clientKafka.emit(process.env.TOPIC_X, payload)` | send | type of `payload` |
| `` clientKafka.emit(`${process.env.TOPIC_X}.dlq`, payload) `` | send, suffix `.dlq` | type of `payload` |
| `this.emitToKafka(this.topicField, key, message)` | send | type of `message` |

The receiver is recognised as a Kafka client when its type is `ClientKafka` or `ClientProxy`, or
its name contains `kafka`. Topic expressions are followed through class fields assigned in the
constructor and through local constants.

Producer sites whose topic is a runtime value (a method parameter, data-driven routing) are not
part of the static contract. They are omitted and listed on stderr.

## Where topic names come from

In a NestJS service the decorator names an **env var**, not a topic. That var is the topic's
identity in code; the string it resolves to is a deployment detail. kafkaui therefore keys every
channel by the env var (`x-env-var`) and fills the `address` from, in order:

1. a literal, when the decorator or emit was given a topic name directly;
2. the project's own declaration: `.env.sample`, `.env.example`, then `.env`;
3. `--env-file`, when given;
4. the live `ConfigService`, when served by `KafkaDocsModule`.

Channels with no declared name stay keyed by env var and are shown as *unresolved* in the viewer.

## Platform view

```bash
kafkaui generate --project ./core       --service core       --out out/core.json
kafkaui generate --project ./game-core  --service game-core  --out out/game-core.json
kafkaui merge out/*.json --title "Event bus" --out out/platform.json --html out/platform.html
```

Merge joins channels by resolved topic name where any service declares one, otherwise by env var,
so a producer and a consumer that name the variable differently still meet on one channel. Every
channel then lists its producers and consumers across services, which is where contract drift
becomes visible: the same topic with `period: string` on one side and `period: RewardPeriodEnum`
on the other.

## CI wiring

```json
{
  "scripts": {
    "docs:kafka": "kafkaui generate --service core --out asyncapi.json"
  }
}
```

```yaml
- run: npm run docs:kafka
- run: git diff --exit-code asyncapi.json   # fails when the contract changed without the doc
```

Commit `asyncapi.json` next to the service; `KafkaDocsModule.setup` serves it.

## Programmatic API

```ts
import { scanProject, toAsyncApi, mergeAsyncApi, renderHtml, KafkaDocsModule } from 'kafkaui';

const doc = toAsyncApi(scanProject({ project: './core', service: 'core' }));
const platform = mergeAsyncApi([doc, other], 'Event bus');
const html = renderHtml(platform);
```

## Compatibility

| kafkaui | TypeScript | Node | NestJS |
|---|---|---|---|
| 0.1.x | 5.x | 18+ | 9, 10 (optional peer) |

`@nestjs/common`, `@nestjs/core`, `@nestjs/config` and `reflect-metadata` are optional peers; the
runtime module resolves them from the host application.

## Limits

- Java (Flink) jobs are not scanned. Their topics appear once a Node service produces or consumes them.
- A handler parameter or emitted value typed `any` shows as "payload type not resolved".
- Object literals with a computed shape are typed as the checker sees them, which is usually fine and occasionally wide.

## License

[MIT](LICENSE)
