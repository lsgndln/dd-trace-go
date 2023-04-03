// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

// Package rueidis provides tracing functions for tracing the rue/redis package (https://github.com/rueian/rueidis).
package rueidis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/rueian/rueidis"
	"github.com/rueian/rueidis/rueidishook"

	"github.com/lsgndln/dd-trace-go/ddtrace"
	"github.com/lsgndln/dd-trace-go/ddtrace/ext"
	"github.com/lsgndln/dd-trace-go/ddtrace/tracer"
)

type datadogHook struct {
	*params
}

// params holds the tracer and a set of parameters which are recorded with every trace.
type params struct {
	config         *clientConfig
	additionalTags []ddtrace.StartSpanOption
}

// WrapClient adds a hook to the given client that traces with the default tracer under the service name "redis".
func WrapClient(client rueidis.Client, addrs []string, opts ...ClientOption) rueidis.Client {
	cfg := new(clientConfig)
	defaults(cfg)
	for _, fn := range opts {
		fn(cfg)
	}

	hookParams := &params{
		additionalTags: additionalTagOptions(addrs),
		config:         cfg,
	}
	return rueidishook.WithHook(client, &datadogHook{params: hookParams})
}

func additionalTagOptions(addrs []string) []ddtrace.StartSpanOption {
	additionalTags := []ddtrace.StartSpanOption{}
	for _, addr := range addrs {
		addrs = append(addrs, addr)
	}
	additionalTags = []ddtrace.StartSpanOption{
		tracer.Tag("addrs", strings.Join(addrs, ", ")),
	}
	return additionalTags
}

func (h *datadogHook) Do(client rueidis.Client, ctx context.Context, cmd rueidishook.Completed) (resp rueidis.RedisResult) {
	ctx, _ = h.start(ctx, completedToStr(cmd), len(cmd.Commands()))
	resp = client.Do(ctx, cmd)
	h.end(ctx, resp.Error())
	return
}

func (h *datadogHook) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidishook.Completed) (resps []rueidis.RedisResult) {
	ctx, _ = h.start(ctx, completedToStr(multi...), len(multi))
	resps = client.DoMulti(ctx, multi...)
	h.end(ctx, firstError(resps))
	return
}

func (h *datadogHook) DoCache(client rueidis.Client, ctx context.Context, cmd rueidishook.Cacheable, ttl time.Duration) (resp rueidis.RedisResult) {
	ctx, _ = h.start(ctx, cacheableToStr(cmd), len(cmd.Commands()))
	resp = client.DoCache(ctx, cmd, ttl)
	h.end(ctx, resp.Error())
	return
}

func (h *datadogHook) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) (resps []rueidis.RedisResult) {
	ctx, _ = h.start(ctx, cacheableTtlToStr(multi...), len(multi))
	resps = client.DoMultiCache(ctx, multi...)
	h.end(ctx, firstError(resps))
	return
}

func (h *datadogHook) Receive(client rueidis.Client, ctx context.Context, subscribe rueidishook.Completed, fn func(msg rueidis.PubSubMessage)) (err error) {
	ctx, _ = h.start(ctx, completedToStr(subscribe), len(subscribe.Commands()))
	err = client.Receive(ctx, subscribe, fn)
	h.end(ctx, err)
	return
}

func (h *datadogHook) start(ctx context.Context, op string, size int) (context.Context, error) {
	p := h.params
	opts := make([]ddtrace.StartSpanOption, 0, 4+1+len(h.additionalTags)+1) // 4 options below + redis.raw_command + h.additionalTags + analyticsRate
	opts = append(opts,
		tracer.SpanType(ext.SpanTypeRedis),
		tracer.ServiceName(p.config.serviceName),
		tracer.ResourceName(resourceName(op)),
		tracer.Tag("redis.args_length", strconv.Itoa(size)),
		tracer.Tag(ext.Component, "rueidis"),
		tracer.Tag(ext.SpanKind, ext.SpanKindClient),
		tracer.Tag(ext.DBSystem, ext.DBSystemRedis),
	)
	if !p.config.skipRaw {
		opts = append(opts, tracer.Tag("redis.raw_command", op))
	}
	opts = append(opts, h.additionalTags...)
	if !math.IsNaN(p.config.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, p.config.analyticsRate))
	}
	_, ctx = tracer.StartSpanFromContext(ctx, "redis.command", opts...)
	return ctx, nil
}

func resourceName(op string) string {
	spaceIndex := strings.IndexByte(op, ' ')
	if spaceIndex > 0 {
		return op[:spaceIndex]
	}
	return op
}

func (h *datadogHook) end(ctx context.Context, errRedis error) {
	var span tracer.Span
	span, _ = tracer.SpanFromContext(ctx)
	var finishOpts []ddtrace.FinishOption
	if errRedis != rueidis.Nil && h.config.errCheck(errRedis) {
		finishOpts = append(finishOpts, tracer.WithError(errRedis))
	}
	span.Finish(finishOpts...)
}

func firstError(s []rueidis.RedisResult) error {
	for _, result := range s {
		if err := result.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return err
		}
	}
	return nil
}

func completedToStr(cmds ...rueidishook.Completed) string {
	var builder strings.Builder
	for _, command := range cmds {
		fmt.Fprint(&builder, strings.Join(command.Commands(), " ")+":\n")
	}
	return builder.String()
}

func cacheableToStr(cmds ...rueidishook.Cacheable) string {
	var builder strings.Builder
	for _, command := range cmds {
		fmt.Fprint(&builder, strings.Join(command.Commands(), " ")+":\n")
	}
	return builder.String()
}

func cacheableTtlToStr(cmds ...rueidis.CacheableTTL) string {
	var builder strings.Builder
	for _, command := range cmds {
		fmt.Fprint(&builder, strings.Join(command.Cmd.Commands(), " ")+":\n")
	}
	return builder.String()
}
