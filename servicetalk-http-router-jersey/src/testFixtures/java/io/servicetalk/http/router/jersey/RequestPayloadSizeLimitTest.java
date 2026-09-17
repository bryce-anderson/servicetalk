/*
 * Copyright © 2026 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.http.router.jersey;

import io.servicetalk.buffer.api.Buffer;
import io.servicetalk.concurrent.api.Publisher;
import io.servicetalk.concurrent.api.Single;
import io.servicetalk.http.api.HttpServerBuilder;
import io.servicetalk.http.utils.PayloadSizeLimitingHttpServiceFilter;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;

import static io.servicetalk.http.api.HttpResponseStatus.OK;
import static io.servicetalk.http.api.HttpResponseStatus.PAYLOAD_TOO_LARGE;
import static java.util.Collections.singleton;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

/**
 * Documents how request payload sizes are bounded for JAX-RS resources: entity readers that buffer the body inside
 * Jersey are not covered by the server's {@code maxAggregatedPayloadSize}, so
 * {@link PayloadSizeLimitingHttpServiceFilter} is the supported way to express a limit.
 */
class RequestPayloadSizeLimitTest extends AbstractJerseyStreamingHttpServiceTest {

    private static final int MAX_PAYLOAD = 16;

    // Applied to the server via configureBuilders(); cleared by the tests that show what the limit does not cover.
    private boolean limitFilter = true;

    @Path("/echo")
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    @SuppressWarnings("PMD.PublicMemberInNonPublicType") // JAX-RS resource must be public
    public static class EchoResource {
        // Jersey's built-in String reader buffers the whole entity inside Jersey.
        @POST
        @Path("/string")
        public String echoString(final String body) {
            return body;
        }

        // Single<Buffer> is aggregated too, but by a ServiceTalk reader.
        @POST
        public Single<Buffer> echo(final Single<Buffer> body) {
            return body;
        }

        // Publisher<Buffer> streams the body rather than buffering it.
        @POST
        @Path("/stream")
        public Publisher<Buffer> echoStream(final Publisher<Buffer> body) {
            return body;
        }
    }

    static class EchoApplication extends Application {
        @Override
        public Set<Object> getSingletons() {
            return singleton(new EchoResource());
        }
    }

    @Override
    protected Application application() {
        return new EchoApplication();
    }

    @Override
    void configureBuilders(final HttpServerBuilder serverBuilder,
                           final HttpJerseyRouterBuilder jerseyRouterBuilder) {
        super.configureBuilders(serverBuilder, jerseyRouterBuilder);
        serverBuilder.maxAggregatedPayloadSize(MAX_PAYLOAD);
        if (limitFilter) {
            serverBuilder.appendServiceFilter(new PayloadSizeLimitingHttpServiceFilter(MAX_PAYLOAD));
        }
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(RouterApi.class)
    void withinLimitSucceeds(final RouterApi api) throws Exception {
        setUp(api);
        final String body = repeat('x', MAX_PAYLOAD);
        sendAndAssertResponse(post("/echo/string", body, TEXT_PLAIN), OK, TEXT_PLAIN, body);
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(RouterApi.class)
    void filterBoundsJerseyAggregatingReader(final RouterApi api) throws Exception {
        setUp(api);
        assertOverLimitRejected("/echo/string");
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(RouterApi.class)
    void filterBoundsServiceTalkAggregatingReader(final RouterApi api) throws Exception {
        setUp(api);
        assertOverLimitRejected("/echo");
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(RouterApi.class)
    void filterBoundsStreamingReader(final RouterApi api) throws Exception {
        // Unlike maxAggregatedPayloadSize, the filter also bounds a body that is never aggregated.
        setUp(api);
        assertOverLimitRejected("/echo/stream");
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(value = RouterApi.class, names = {"ASYNC_STREAMING", "BLOCKING_STREAMING"})
    void withoutFilterJerseyAggregationIsUnbounded(final RouterApi api) throws Exception {
        // Jersey buffers the String entity itself, which maxAggregatedPayloadSize does not cover: without the filter an
        // oversized body is served. This is why the filter is the documented way to limit payload sizes.
        limitFilter = false;
        setUp(api);
        final String body = repeat('x', MAX_PAYLOAD * 4);
        sendAndAssertResponse(post("/echo/string", body, TEXT_PLAIN), OK, TEXT_PLAIN, body);
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(value = RouterApi.class, names = {"ASYNC_AGGREGATED", "BLOCKING_AGGREGATED"})
    void withoutFilterServiceTalkAggregationIsBounded(final RouterApi api) throws Exception {
        // The aggregated paradigms aggregate in the transport, where maxAggregatedPayloadSize does apply.
        limitFilter = false;
        setUp(api);
        assertOverLimitRejected("/echo");
    }

    private void assertOverLimitRejected(final String path) {
        try {
            sendAndAssertStatusOnly(post(path, repeat('x', MAX_PAYLOAD + 1), TEXT_PLAIN), PAYLOAD_TOO_LARGE);
        } catch (RuntimeException e) {
            // Rejecting an oversized body tears down the in-flight upload, so a connection teardown may race ahead of
            // the mapped 413; both mean the payload was rejected. A timeout instead indicates a hang and must fail.
            if (e.getCause() instanceof TimeoutException) {
                throw e;
            }
        }
    }

    private static String repeat(final char c, final int n) {
        final char[] chars = new char[n];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
