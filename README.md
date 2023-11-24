TODO:

## Missing features

- [ ] Mimic transaction behaviour from cloud Firestore (mixed pessimistic and optimistic)
- [ ] Detect idempotent updates and do not update `update_time` in those cases
- [ ] Rest of the updates with Transforms
- [ ] Documentation
- [ ] live queries
- [ ] ...

## Important to know

### DashMap

Internally we use `DashMap` for easy and performant concurrent access to a single HashMap. Unfortunately, as of now, DashMap is not built with async in mind and can deadlock if a ref is kept across an await point. Take care

## How to use

```shell
brew install protobuf
```

### Development

Debug mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo watch -cx run
```

Release mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo watch -cx "run --release"
```

### Install

```shell
cargo install --path .
```

Then run:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 firestore-emulator
```
