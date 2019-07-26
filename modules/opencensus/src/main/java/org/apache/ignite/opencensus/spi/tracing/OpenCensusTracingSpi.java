package org.apache.ignite.opencensus.spi.tracing;

import io.opencensus.internal.DefaultVisibilityForTesting;
import io.opencensus.internal.Provider;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.TraceTags;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    private OpenCensusTraceExporter exporter;

    private TraceComponent traceComponent;

    public OpenCensusTracingSpi() {
    }

    public OpenCensusTracingSpi(TraceComponent traceComponent) {
        this.traceComponent = traceComponent;
    }

    public OpenCensusTracingSpi withExporter(OpenCensusTraceExporter exporter) {
        this.exporter = exporter;

        return this;
    }

    /**
     *
     */
    @DefaultVisibilityForTesting
    OpenCensusTraceExporter getExporter() {
        return exporter;
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter create(@NotNull String name, @Nullable Span parentSpan) {
        try {
            SpanAdapter spanAdapter = (SpanAdapter) parentSpan;

            return new SpanAdapter(
                traceComponent.getTracer().spanBuilderWithExplicitParent(name, spanAdapter != null ? spanAdapter.impl() : null)
                    .setSampler(Samplers.alwaysSample())
                    .startSpan()
            ).addTag(TraceTags.tag(TraceTags.NODE, TraceTags.NAME), igniteInstanceName);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to create from parent", e);
        }
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        try {
            return new SpanAdapter(
                traceComponent.getTracer().spanBuilderWithRemoteParent(
                    name,
                    traceComponent.getPropagationComponent().getBinaryFormat().fromByteArray(serializedSpanBytes)
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            ).addTag(TraceTags.tag(TraceTags.NODE, TraceTags.NAME), igniteInstanceName);
        }
        catch (SpanContextParseException e) {
            throw new IgniteException("Failed to create span from serialized value: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        SpanAdapter spanAdapter = (SpanAdapter) span;

        return traceComponent.getPropagationComponent().getBinaryFormat().toByteArray(spanAdapter.impl().getContext());
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (traceComponent == null) {
            traceComponent = createTraceComponent(getClass().getClassLoader());

            if (exporter != null)
                exporter.start(traceComponent, igniteInstanceName);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (exporter != null)
            exporter.stop(traceComponent);
    }

    /**
     * Any provider that may be used for TraceComponent can be added here.
     *
     * @param classLoader Class loader.
     */
    public static TraceComponent createTraceComponent(@javax.annotation.Nullable ClassLoader classLoader) {
        // Exception that contains possible causes of failed TraceComponent start.
        IgniteSpiException startE;

        try {
            // Call Class.forName with literal string name of the class to help shading tools.
            return Provider.createInstance(
                Class.forName(
                    "io.opencensus.impl.trace.TraceComponentImpl", /*initialize=*/ true, classLoader),
                TraceComponent.class);
        }
        catch (Exception e) {
            startE = new IgniteSpiException("Failed to start TraceComponent.");

            startE.addSuppressed(e);
        }

        try {
            // Call Class.forName with literal string name of the class to help shading tools.
            return Provider.createInstance(
                Class.forName(
                    "io.opencensus.impllite.trace.TraceComponentImplLite",
                    /*initialize=*/ true,
                    classLoader),
                TraceComponent.class);
        }
        catch (Exception e) {
            startE.addSuppressed(e);
        }

        throw startE;
    }
}
