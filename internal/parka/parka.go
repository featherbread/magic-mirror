/*
Package parka provides managed concurrency abstractions for keyable work.

Parka's [Map] composes duplicate call suppression, caching, and bounded
concurrency into a single mechanism for distributing work across goroutines.
Maps provide substantial dynamic control over concurrency limits and task
prioritization, including at the level of individual running handlers.

This version of Parka is suited for work that does not require cache eviction,
such as a main package running a single batch-type workflow or short-lived maps
that are disposed of after a single use.
*/
package parka
