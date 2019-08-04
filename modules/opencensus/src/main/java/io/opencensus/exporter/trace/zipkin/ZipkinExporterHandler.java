/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opencensus.exporter.trace.zipkin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.opencensus.common.Duration;
import io.opencensus.common.Function;
import io.opencensus.common.Functions;
import io.opencensus.common.Timestamp;
import io.opencensus.exporter.trace.TimeLimitedHandler;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span.Kind;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanData.TimedEvent;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Sender;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Copy-pasted from OpenCensus library handler with ability to set external Tracer.
 */
public final class ZipkinExporterHandler extends TimeLimitedHandler {
    /** Logger. */
    private static final Logger logger = Logger.getLogger(ZipkinExporterHandler.class.getName());
    /** Export span name. */
    private static final String EXPORT_SPAN_NAME = "SendZipkinSpans";
    /** SpanStatus code. */
    private static final String STATUS_CODE = "census.status_code";
    /** SpanStatus description. */
    private static final String STATUS_DESCRIPTION = "census.status_description";
    /** Encoder. */
    private final SpanBytesEncoder encoder;
    /** Sender. */
    private final Sender sender;
    /** Local endpoint. */
    private final Endpoint localEndpoint;

    /**
     * @param tracer Tracer.
     * @param encoder Encoder.
     * @param sender Sender.
     * @param serviceName Service name.
     * @param deadline Deadline.
     */
    ZipkinExporterHandler(
        Tracer tracer, SpanBytesEncoder encoder, Sender sender, String serviceName, Duration deadline) {
        super(tracer, deadline, EXPORT_SPAN_NAME);
        this.encoder = encoder;
        this.sender = sender;
        this.localEndpoint = produceLocalEndpoint(serviceName);
    }

    /** Logic borrowed from brave.internal.Platform.produceLocalEndpoint */
    static Endpoint produceLocalEndpoint(String serviceName) {
        Endpoint.Builder builder = Endpoint.newBuilder().serviceName(serviceName);
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            if (nics == null)
                return builder.build();
            while (nics.hasMoreElements()) {
                NetworkInterface nic = nics.nextElement();
                Enumeration<InetAddress> addresses = nic.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isSiteLocalAddress()) {
                        builder.ip(address);
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            // don't crash the caller if there was a problem reading nics.
            if (logger.isLoggable(Level.FINE))
                logger.log(Level.FINE, "error reading nics", e);
        }
        return builder.build();
    }

    /**
     * @param spanData Span data.
     * @param localEndpoint Local endpoint.
     */
    @SuppressWarnings("deprecation")
    static Span generateSpan(SpanData spanData, Endpoint localEndpoint) {
        SpanContext context = spanData.getContext();
        long startTimestamp = toEpochMicros(spanData.getStartTimestamp());

        // TODO(sebright): Fix the Checker Framework warning.
        @SuppressWarnings("nullness")
        long endTimestamp = toEpochMicros(spanData.getEndTimestamp());

        // TODO(bdrutu): Fix the Checker Framework warning.
        @SuppressWarnings("nullness")
        Span.Builder spanBuilder =
            Span.newBuilder()
                .traceId(context.getTraceId().toLowerBase16())
                .id(context.getSpanId().toLowerBase16())
                .kind(toSpanKind(spanData))
                .name(spanData.getName())
                .timestamp(toEpochMicros(spanData.getStartTimestamp()))
                .duration(endTimestamp - startTimestamp)
                .localEndpoint(localEndpoint);

        if (spanData.getParentSpanId() != null && spanData.getParentSpanId().isValid())
            spanBuilder.parentId(spanData.getParentSpanId().toLowerBase16());

        for (Map.Entry<String, AttributeValue> label :
            spanData.getAttributes().getAttributeMap().entrySet())
                spanBuilder.putTag(label.getKey(), attributeValueToString(label.getValue()));
        Status status = spanData.getStatus();
        if (status != null) {
            spanBuilder.putTag(STATUS_CODE, status.getCanonicalCode().toString());
            if (status.getDescription() != null)
                spanBuilder.putTag(STATUS_DESCRIPTION, status.getDescription());
        }

        for (TimedEvent<Annotation> annotation : spanData.getAnnotations().getEvents()) {
            spanBuilder.addAnnotation(
                toEpochMicros(annotation.getTimestamp()), annotation.getEvent().getDescription());
        }

        for (TimedEvent<io.opencensus.trace.MessageEvent> messageEvent :
            spanData.getMessageEvents().getEvents()) {
            spanBuilder.addAnnotation(
                toEpochMicros(messageEvent.getTimestamp()), messageEvent.getEvent().getType().name());
        }

        return spanBuilder.build();
    }

    /**
     * @param spanData Span data.
     */
    @javax.annotation.Nullable
    private static Span.Kind toSpanKind(SpanData spanData) {
        // This is a hack because the Span API did not have SpanKind.
        if (spanData.getKind() == Kind.SERVER
            || (spanData.getKind() == null && Boolean.TRUE.equals(spanData.getHasRemoteParent())))
            return Span.Kind.SERVER;

        // This is a hack because the Span API did not have SpanKind.
        if (spanData.getKind() == Kind.CLIENT || spanData.getName().startsWith("Sent."))
            return Span.Kind.CLIENT;

        return null;
    }

    /**
     * @param timestamp Timestamp.
     */
    private static long toEpochMicros(Timestamp timestamp) {
        return SECONDS.toMicros(timestamp.getSeconds()) + NANOSECONDS.toMicros(timestamp.getNanos());
    }


    /** The return type needs to be nullable when this function is used as an argument to 'match' in
        attributeValueToString, because 'match' doesn't allow covariant return types. */
    private static final Function<Object, /*@Nullable*/ String> returnToString =
        Functions.returnToString();

    // TODO: Fix the Checker Framework warning.
    /**
     * @param attributeValue Attribute value.
     */
    @SuppressWarnings("nullness")
    private static String attributeValueToString(AttributeValue attributeValue) {
        return attributeValue.match(
            returnToString,
            returnToString,
            returnToString,
            returnToString,
            Functions.<String>returnConstant(""));
    }

    /** {@inheritDoc} */
    @Override public void timeLimitedExport(final Collection<SpanData> spanDataList) throws IOException {
        List<byte[]> encodedSpans = new ArrayList<byte[]>(spanDataList.size());
        for (SpanData spanData : spanDataList)
            encodedSpans.add(encoder.encode(generateSpan(spanData, localEndpoint)));
        sender.sendSpans(encodedSpans).execute();
    }
}
