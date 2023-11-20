TODO:

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
