# Rust Client Libraries for Modality

These are a set of libraries for the rust programming language used to
interact with Auxon's Modality suite of products.

## [`modality-api`](./modality-api/)

The base types used throughout the rest of these libraries.

## [`modality-auth-token`](./modality-auth-token)

Provides standardized access to local modality auth tokens.

## [`modality-ingest-client`](./modality-ingest-client/)

A client library for the Modality ingest plane protocol, allowing you to
easily create custom trace data ingest integrations.

## [`modality-ingest-protocol`](./modality-ingest-protocol/)

The protocol definition of the Modality ingest plane.

## [`modality-mutation-plane`](./modality-mutation-plane/)

The protocol definition of the Deviant mutation control plane.

## [`modality-mutation-plane-client`](./modality-mutation-plane-client/)

Client library to participate in the Deviant mutation control plane.

## [`modality-mutator-protocol`](./modality-mutator-protocol/)

The mutator part of the Deviant mutation control plane protocol defintion.

## [`modality-mutator-server`](./modality-mutator-server/)

An HTTP server template that can be used to serve mutators to
Modality.

## [`modality-plugin-utils`](./modality-plugin-utils)

Various helpful utilities for writing modality-reflector plugins.

## [`modality-reflector-config`](./modality-reflector-config/)

A format definition and parser for the `modality-reflector`, to be used
by custom reflector plugins that can be hosted within the reflector
itself.
