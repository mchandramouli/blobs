package com.expedia.www.haystack.client

import java.util

object MockSpanBuilder {
  def mockSpan(tracer: Tracer, clock: Clock, operationName: String, context: SpanContext,
               startTime: Long, tags: util.Map[String, Object], references: util.List[Reference]) : Span = {
    new Span(tracer, clock, "span-name", context,
      System.currentTimeMillis(), tags, new util.ArrayList[Reference])
  }
}
