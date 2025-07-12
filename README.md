# Haggen

Haggen is a reliable and fault-tolerant Postgres backed job queue
written in Java.

## Features

Haggen's core features and reliability is based on very simple principles :

* **Transactional enqueueing** allows you get transactional semantics as part of your application logic.
* **Transactional and Non-Transactional workers** your jobs can run inside a transaction, for short-lived and critical
  workloads this allows you to get idempotency and reliability out of the box. Non transactional workers for long-lived
  jobs get the benefit of a lease based system that ensures non-idempotent workloads don't get retried or lost when
  workers die.
* **Natively distributed** By leveraging Postgres advisory locks Haggen can scale horizontally across multiple services
  without requiring any added deployments or services.
* **Designed by API** Haggen allows you to enqueue jobs via a single method call or via a gRPC API.

