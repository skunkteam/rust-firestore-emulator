# Firestore Emulator

A local, Rust-based Google Cloud Firestore emulator built with a focus on stability, performance, and exact cloud-compliance for extensive unit testing and CI environments.

## Overview

This repository provides a powerful alternative to the official Java-based Firebase emulator for Firestore. It is designed to act as a robust backend for running large and concurrent test suites, avoiding the instabilities and out-of-spec edgecases occasionally found in other local emulators.

### Key Characteristics

- **Strict Concurrency Simulation**: Utilizes a custom `fifo-rwlock` underneath the hood. This was strictly necessary because standard async read-write locks do not provide the proper characteristics to emulate the exact locking and concurrency behavior of Cloud Firestore.
- **Driven by Need**: The project was born out of internal testing and usage requirements. Therefore, it is exceptionally complete for the majority of core usecases (queries, transactions, collections, documents). 
- **Java Emulator Discrepancies**: It was built bearing in mind that the official Java emulator is definitely not fully spec-compliant. This Rust variant aims to align strictly with the actual Cloud behavior instead.

## Missing Features

Because the implementation is driven by the features we consume, the emulator is practically complete for standard use, but some advanced or edge-case features are still missing:

- Some of the updates with Transforms (such as Maximum and Minimum)
- Multiplexing listeners in a single stream (used by frontend Firestore SDK)
- Nearest neighbors search
- Some field filters (such as ArrayContainsAny and NotIn)
- Explain options
- and many more... search for `unimplemented` in the codebase to get an idea of the unimplemented gRPC endpoints or edge cases.

## Usage & Development Workflow

### Prerequisites

```shell
brew install protobuf
```

For executing the test-suite and development tools `just` and `node` are recommended:

```shell
brew install just node
```

### Building and Testing

The full suite of tests (both Rust and Node.js SDK tests), including linting, can be natively executed using a single command:

```shell
just
```

To run `just` in watch-mode, automatically re-running on file modifications:

```shell
just watch
```

### Ground-truth Testing against the Cloud

The test-suite located in the `test-suite/` directory is an expansive verification suite. By default, it runs against the local emulator. But it can also be configured to run against the real Google Cloud Firestore backend directly! This allows us to quickly verify if the emulator diverges from reality.

Run the tests against the cloud directly (requires GCP CLI authentication):

```shell
cd test-suite/
npm test
```

### Standalone Server Starts

Run locally in debug-mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run
```

Run locally in release-mode:

```shell
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo run --release
```

To install directly to cargo:

```shell
cargo install --path .
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 firestore-emulator
```
