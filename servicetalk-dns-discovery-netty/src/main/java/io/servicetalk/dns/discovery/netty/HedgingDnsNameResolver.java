/*
 * Copyright © 2024 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.dns.discovery.netty;

import io.servicetalk.concurrent.Cancellable;

import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.servicetalk.transport.api.IoExecutor;
import io.servicetalk.transport.netty.internal.EventLoopAwareNettyIoExecutor;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.servicetalk.transport.netty.internal.EventLoopAwareNettyIoExecutors.toEventLoopAwareNettyIoExecutor;
import static io.servicetalk.utils.internal.NumberUtils.ensurePositive;
import static java.lang.Math.max;
import static java.lang.Math.min;

final class HedgingDnsNameResolver implements UnderlyingDnsResolver {

    private final UnderlyingDnsResolver delegate;
    private final EventLoopAwareNettyIoExecutor executor;
    private final PercentileTracker percentile;
    private final Budget budget;

    HedgingDnsNameResolver(DnsNameResolver delegate, IoExecutor executor) {
        this(new NettyDnsNameResolver(delegate), executor);
    }

    HedgingDnsNameResolver(UnderlyingDnsResolver delegate, IoExecutor executor) {
        this(delegate, executor, defaultTracker(), defaultBudget());
    }

    HedgingDnsNameResolver(UnderlyingDnsResolver delegate, IoExecutor executor,
                           PercentileTracker percentile, Budget budget) {
        this.delegate = delegate;
        this.executor = toEventLoopAwareNettyIoExecutor(executor).next();
        this.percentile = percentile;
        this.budget = budget;
    }

    @Override
    public long queryTimeoutMillis() {
        return delegate.queryTimeoutMillis();
    }

    @Override
    public Future<List<DnsRecord>> resolveAllQuestion(DnsQuestion t) {
        return setupHedge(delegate::resolveAllQuestion, t);
    }

    @Override
    public Future<List<InetAddress>> resolveAll(String t) {
        return setupHedge(delegate::resolveAll, t);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private long currentTimeMillis() {
        return executor.currentTime(TimeUnit.MILLISECONDS);
    }

    private <T, R> Future<R> setupHedge(Function<T, Future<R>> computation, T t) {
        // Only add tokens for organic requests and not retries.
        budget.deposit();
        Future<R> underlyingResult = computation.apply(t);
        final long delay = percentile.getValue();
        if (delay == Long.MAX_VALUE) {
            // basically forever: just return the value.
            return underlyingResult;
        } else {
            final long startTimeMs = currentTimeMillis();
            Promise<R> promise = executor.eventLoopGroup().next().newPromise();
            Cancellable hedgeTimer = executor.schedule(() -> tryHedge(computation, t, underlyingResult, promise),
                    delay, TimeUnit.MILLISECONDS);
            underlyingResult.addListener(completedFuture -> {
                measureRequest(currentTimeMillis() - startTimeMs, completedFuture);
                if (complete(underlyingResult, promise)) {
                    hedgeTimer.cancel();
                }
            });
            return promise;
        }
    }

    private <T, R> void tryHedge(
            Function<T, Future<R>> computation, T t, Future<R> original, Promise<R> promise) {
        if (!original.isDone() && budget.withdraw()) {
            System.out.println("" + System.currentTimeMillis() + ": sending backup request.");
            Future<R> backupResult = computation.apply(t);
            final long startTime = currentTimeMillis();
            backupResult.addListener(done -> {
                if (complete(backupResult, promise)) {
                    original.cancel(true);
                    measureRequest(currentTimeMillis() - startTime, done);
                }
            });
            promise.addListener(complete -> backupResult.cancel(true));
        }
    }

    private void measureRequest(long durationMs, Future<?> future) {
        // Cancelled responses don't count but we do consider failed responses because failure
        // is a legitimate response.
        if (!future.isCancelled()) {
            percentile.addSample(durationMs);
        }
    }

    private <T, R> boolean complete(Future<R> f, Promise<R> p) {
        assert f.isDone();
        if (f.isSuccess()) {
            return p.trySuccess(f.getNow());
        } else {
            return p.tryFailure(f.cause());
        }
    }

    interface PercentileTracker {
        void addSample(long sample);

        long getValue();
    }

    interface Budget {
        void deposit();

        boolean withdraw();
    }

    private static final class DefaultPercentileTracker implements PercentileTracker {

        private final MovingVariance movingVariance;
        private final double multiple;

        DefaultPercentileTracker(final double multiple, final int historySize) {
            movingVariance = new MovingVariance(historySize);
            this.multiple = multiple;
        }

        @Override
        public void addSample(long sample) {
            int clipped = Math.max(0, sample > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sample);
            movingVariance.addSample(clipped);
        }

        @Override
        public long getValue() {
            return Math.round(movingVariance.mean() + movingVariance.stdev() * multiple);
        }
    }

    private static final class DefaultBudgetImpl implements Budget {

        private final int depositAmount;
        private final int withDrawAmount;
        private final int maxTokens;
        private int tokens;

        DefaultBudgetImpl(int depositAmount, int withDrawAmount, int maxTokens) {
            this(depositAmount, withDrawAmount, maxTokens, 0);
        }

        DefaultBudgetImpl(int depositAmount, int withDrawAmount, int maxTokens, int initialTokens) {
            this.depositAmount = depositAmount;
            this.withDrawAmount = withDrawAmount;
            this.maxTokens = maxTokens;
            this.tokens = initialTokens;
        }

        @Override
        public void deposit() {
            tokens = max(maxTokens, tokens + depositAmount);
        }

        @Override
        public boolean withdraw() {
            if (tokens < withDrawAmount) {
                return false;
            } else {
                tokens -= withDrawAmount;
                return true;
            }
        }
    }

    private static PercentileTracker defaultTracker() {
        return new DefaultPercentileTracker(3.0, 256);
    }

    private static Budget defaultBudget() {
        // 5% extra load and a max burst of 5 hedges.
        return new DefaultBudgetImpl(1, 20, 100);
    }

    static PercentileTracker constantTracker(int value) {
        return new PercentileTracker() {
            @Override
            public void addSample(long sample) {
                // noop
            }

            @Override
            public long getValue() {
                return value;
            }
        };
    }

    static Budget alwaysAllowBudget() {
        return new Budget() {
            @Override
            public void deposit() {
                // noop
            }

            @Override
            public boolean withdraw() {
                return true;
            }
        };
    }
}
