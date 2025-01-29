## Missing features

- [ ] Some of the updates with Transforms (such as Maximum and Minimum)
- [ ] Multiplexing listeners in a single stream (used by frontend Firestore SDK)
- [ ] Nearest neighbors search
- [ ] Some field filters (such as ArrayContainsAny and NotIn)
- [ ] Some API's that are not used by NodeJS SDK anymore (such as CreateDocument and Updatedocument)
- [ ] Explain options
- [ ] and many more... search for `unimplemented` to get the idea

And also missing a lot of documentation. 

## Important to know

This is highly experimental software. We use this in our dev and CI enviroments to get better stability (and performance) while running our extensive unit test suites.

## How to use

### Prerequisites

```shell
brew install protobuf
```

### Development

Debug mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run
```

Release mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run --release
```

### Install

```shell
cargo install --path .
```

Then run:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 firestore-emulator
```
