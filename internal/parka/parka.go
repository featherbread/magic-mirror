/*
Package parka provides managed concurrency abstractions for keyable work.

Parka's core abstraction, the [Map], composes duplicate call suppression,
caching, and bounded concurrency into a single "lazy map" concept, eliminating
traditional boilerplate for distributing work across goroutines. Maps provide
substantial dynamic control over concurrency limits and task prioritization,
including at the level of individual running handlers.

This version of Parka is best suited for work that does not require task
cancellation or cache eviction, such as a main package that runs a single
batch-type workflow and exits immediately on success or error.
*/
package parka
